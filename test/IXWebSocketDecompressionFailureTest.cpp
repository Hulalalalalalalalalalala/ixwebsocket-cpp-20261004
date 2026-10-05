/*
 * IXWebSocketDecompressionFailureTest.cpp
 *
 * Verifies that a message whose compressed payload is rejected by zlib fails
 * the whole message and tears down the connection, on both client and server
 * sides, for both text and binary messages. Also verifies that valid
 * compressed messages (empty, larger than 32KiB, fragmented, using context
 * takeover) keep being delivered correctly, and that a later reconnect on the
 * same WebSocket object starts from a clean state.
 */

#include "IXTest.h"
#include "catch_amalgamated.hpp"
#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <ixwebsocket/IXGetFreePort.h>
#include <ixwebsocket/IXWebSocket.h>
#include <ixwebsocket/IXWebSocketCloseConstants.h>
#include <ixwebsocket/IXWebSocketHandshakeKeyGen.h>
#include <ixwebsocket/IXWebSocketPerMessageDeflateCodec.h>
#include <ixwebsocket/IXWebSocketServer.h>
#include <list>
#include <mutex>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include <zlib.h>

using namespace ix;

namespace
{
    //
    // A minimal raw WebSocket peer speaking permessage-deflate over TCP.
    //
    class RawPeer
    {
    public:
        ~RawPeer()
        {
            reset();
        }

        // Listen so a websocket *client* can connect to this peer (peer acts as
        // a permessage-deflate server).
        bool listen()
        {
            _listenFd = ::socket(AF_INET, SOCK_STREAM, 0);
            REQUIRE(_listenFd >= 0);
            int yes = 1;
            setsockopt(_listenFd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

            sockaddr_in addr {};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            addr.sin_port = 0;
            REQUIRE(bind(_listenFd, (sockaddr*) &addr, sizeof(addr)) == 0);
            REQUIRE(::listen(_listenFd, 5) == 0);

            socklen_t len = sizeof(addr);
            REQUIRE(getsockname(_listenFd, (sockaddr*) &addr, &len) == 0);
            _port = ntohs(addr.sin_port);
            return true;
        }

        int port() const
        {
            return _port;
        }

        // Block until one client has connected and finish the handshake (the
        // raw peer is the WebSocket server side of this exchange).
        bool acceptWithDeflate(const std::string& responseExtensions)
        {
            _fd = ::accept(_listenFd, nullptr, nullptr);
            REQUIRE(_fd >= 0);
            setNoDelay();
            return finishServerHandshake(responseExtensions);
        }

        bool finishServerHandshake(const std::string& responseExtensions)
        {
            std::string request;
            char buf[1024];
            while (request.find("\r\n\r\n") == std::string::npos)
            {
                ssize_t n = ::recv(_fd, buf, sizeof(buf), 0);
                REQUIRE(n > 0);
                request.append(buf, (size_t) n);
            }

            std::string key;
            const std::string keyHeader = "Sec-WebSocket-Key:";
            auto pos = request.find(keyHeader);
            REQUIRE(pos != std::string::npos);
            pos += keyHeader.size();
            while (pos < request.size() && (request[pos] == ' ' || request[pos] == '\t')) ++pos;
            auto end = request.find("\r\n", pos);
            key = request.substr(pos, end - pos);

            char accept[29] = {};
            WebSocketHandshakeKeyGen::generate(key, accept);

            std::string response = "HTTP/1.1 101 Switching Protocols\r\n"
                                   "Upgrade: websocket\r\n"
                                   "Connection: Upgrade\r\n"
                                   "Sec-WebSocket-Accept: " +
                                   std::string(accept) + "\r\n";
            if (!responseExtensions.empty())
            {
                response += "Sec-WebSocket-Extensions: " + responseExtensions + "\r\n";
            }
            response += "\r\n";

            REQUIRE(sendAll(response));
            return true;
        }

        // Connect this peer to a websocket *server* and perform the client
        // handshake requesting permessage-deflate.
        bool connect(int port)
        {
            _fd = ::socket(AF_INET, SOCK_STREAM, 0);
            REQUIRE(_fd >= 0);
            setNoDelay();

            sockaddr_in addr {};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            addr.sin_port = htons((uint16_t) port);
            REQUIRE(::connect(_fd, (sockaddr*) &addr, sizeof(addr)) == 0);

            std::string request =
                "GET / HTTP/1.1\r\n"
                "Host: localhost\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                "Sec-WebSocket-Version: 13\r\n"
                "Sec-WebSocket-Extensions: permessage-deflate\r\n"
                "\r\n";
            REQUIRE(sendAll(request));

            std::string response;
            char buf[1024];
            while (response.find("\r\n\r\n") == std::string::npos)
            {
                ssize_t n = ::recv(_fd, buf, sizeof(buf), 0);
                REQUIRE(n > 0);
                response.append(buf, (size_t) n);
            }
            REQUIRE(response.find("101") != std::string::npos);
            REQUIRE(response.find("permessage-deflate") != std::string::npos);
            return true;
        }

        // Build a websocket frame.
        static std::string frame(int opcode,
                                 bool fin,
                                 bool rsv1,
                                 const std::string& payload,
                                 bool mask)
        {
            std::string out;
            uint8_t b0 = (uint8_t) opcode;
            if (fin) b0 |= 0x80;
            if (rsv1) b0 |= 0x40;
            out.push_back((char) b0);

            uint8_t maskBit = mask ? 0x80 : 0;
            uint64_t n = payload.size();
            if (n < 126)
            {
                out.push_back((char) (maskBit | (uint8_t) n));
            }
            else if (n <= 0xffff)
            {
                out.push_back((char) (maskBit | 126));
                out.push_back((char) ((n >> 8) & 0xff));
                out.push_back((char) (n & 0xff));
            }
            else
            {
                out.push_back((char) (maskBit | 127));
                for (int i = 7; i >= 0; --i)
                {
                    out.push_back((char) ((n >> (8 * i)) & 0xff));
                }
            }

            uint8_t key[4] = {0x12, 0x34, 0x56, 0x78};
            if (mask)
            {
                for (int i = 0; i < 4; ++i) out.push_back((char) key[i]);
            }

            for (uint64_t i = 0; i < n; ++i)
            {
                char c = payload[i];
                if (mask) c ^= (char) key[i & 3];
                out.push_back(c);
            }
            return out;
        }

        bool sendAll(const std::string& data)
        {
            size_t sent = 0;
            while (sent < data.size())
            {
                ssize_t n = ::send(_fd, data.data() + sent, data.size() - sent, 0);
                if (n <= 0) return false;
                sent += (size_t) n;
            }
            return true;
        }

        bool sendFrame(int opcode,
                       bool fin,
                       bool rsv1,
                       const std::string& payload,
                       bool mask)
        {
            return sendAll(frame(opcode, fin, rsv1, payload, mask));
        }

        // Send the given bytes one at a time, to exercise header/payload
        // reads that arrive in several socket reads.
        void sendByteByByte(const std::string& data)
        {
            for (char c : data)
            {
                REQUIRE(sendAll(std::string(1, c)));
                ix::msleep(1);
            }
        }

        struct ParsedFrame
        {
            bool fin;
            bool rsv1;
            int opcode;
            std::string payload;
        };

        // Read one frame (blocking). Returns false on clean/error close.
        bool recvFrame(ParsedFrame& frame)
        {
            uint8_t h[2];
            if (!recvExact(h, 2)) return false;
            frame.fin = (h[0] & 0x80) != 0;
            frame.rsv1 = (h[0] & 0x40) != 0;
            frame.opcode = h[0] & 0x0f;
            bool masked = (h[1] & 0x80) != 0;
            uint64_t n = h[1] & 0x7f;
            if (n == 126)
            {
                uint8_t e[2];
                if (!recvExact(e, 2)) return false;
                n = ((uint64_t) e[0] << 8) | e[1];
            }
            else if (n == 127)
            {
                uint8_t e[8];
                if (!recvExact(e, 8)) return false;
                n = 0;
                for (int i = 0; i < 8; ++i) n = (n << 8) | e[i];
            }
            uint8_t key[4] = {};
            if (masked)
            {
                if (!recvExact(key, 4)) return false;
            }
            frame.payload.resize((size_t) n);
            if (n > 0 && !recvExact((uint8_t*) &frame.payload[0], (size_t) n)) return false;
            if (masked)
            {
                for (uint64_t i = 0; i < n; ++i) frame.payload[i] ^= (char) key[i & 3];
            }
            return true;
        }

        void closePeer()
        {
            if (_fd >= 0)
            {
                ::shutdown(_fd, SHUT_RDWR);
                ::close(_fd);
                _fd = -1;
            }
        }

        void reset()
        {
            closePeer();
            if (_listenFd >= 0)
            {
                ::close(_listenFd);
                _listenFd = -1;
            }
        }

        // Send back a CLOSE frame echoing code and reason.
        void sendCloseEcho(uint16_t code, const std::string& reason, bool mask)
        {
            std::string payload;
            payload.push_back((char) ((code >> 8) & 0xff));
            payload.push_back((char) (code & 0xff));
            payload += reason;
            sendFrame(8, true, false, payload, mask);
        }

    private:
        void setNoDelay()
        {
            int yes = 1;
            setsockopt(_fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
        }

        bool recvExact(uint8_t* dst, size_t n)
        {
            size_t got = 0;
            while (got < n)
            {
                ssize_t r = ::recv(_fd, dst + got, n - got, 0);
                if (r <= 0) return false;
                got += (size_t) r;
            }
            return true;
        }

        int _listenFd = -1;
        int _fd = -1;
        int _port = 0;
    };

    //
    // Raw DEFLATE compression with a reusable context (context takeover),
    // matching permessage-deflate without *_no_context_takeover.
    //
    class RawDeflater
    {
    public:
        RawDeflater()
        {
            memset(&_stream, 0, sizeof(_stream));
            REQUIRE(deflateInit2(&_stream,
                                Z_DEFAULT_COMPRESSION,
                                Z_DEFLATED,
                                -15,
                                4,
                                Z_DEFAULT_STRATEGY) == Z_OK);
        }

        ~RawDeflater()
        {
            deflateEnd(&_stream);
        }

        std::string compress(const std::string& in)
        {
            std::vector<uint8_t> out(in.size() + 64);
            _stream.avail_in = (uInt) in.size();
            _stream.next_in = (const_cast<uint8_t*>((const uint8_t*) in.data()));
            _stream.avail_out = (uInt) out.size();
            _stream.next_out = out.data();
            REQUIRE(deflate(&_stream, Z_SYNC_FLUSH) == Z_OK);
            size_t n = out.size() - _stream.avail_out;
            // strip the trailing 00 00 ff ff empty block marker
            REQUIRE(n >= 4);
            n -= 4;
            return std::string((char*) out.data(), n);
        }

    private:
        z_stream _stream;
    };

    struct Event
    {
        WebSocketMessageType type;
        std::string str;
        size_t wireSize = 0;
        bool binary = false;
        bool decompressionError = false;
        std::string reason;
        uint16_t closeCode = 0;
        std::string closeReason;
        bool closeRemote = false;
    };

    struct EventLog
    {
        std::mutex mutex;
        std::list<Event> events;
        std::atomic<bool> sendInErrorCallbackFailed {false};
        std::atomic<bool> ready {false};
        WebSocket* ws = nullptr;

        void push(const WebSocketMessagePtr& msg)
        {
            Event e;
            e.type = msg->type;
            e.str = msg->str;
            e.wireSize = msg->wireSize;
            e.binary = msg->binary;
            e.decompressionError = msg->errorInfo.decompressionError;
            e.reason = msg->errorInfo.reason;
            e.closeCode = msg->closeInfo.code;
            e.closeReason = msg->closeInfo.reason;
            e.closeRemote = msg->closeInfo.remote;

            if (msg->type == WebSocketMessageType::Error &&
                msg->errorInfo.decompressionError && ws)
            {
                // Sending from within the error callback must fail: the
                // connection must already be closing.
                auto info = ws->send("should-fail", false);
                sendInErrorCallbackFailed = !info.success;
            }

            std::lock_guard<std::mutex> lock(mutex);
            events.push_back(e);
            if (msg->type == WebSocketMessageType::Open) ready = true;
        }

        std::vector<Event> snapshot()
        {
            std::lock_guard<std::mutex> lock(mutex);
            return std::vector<Event>(events.begin(), events.end());
        }

        bool waitFor(WebSocketMessageType type, int timeoutMs = 5000)
        {
            auto deadline = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(timeoutMs);
            while (std::chrono::steady_clock::now() < deadline)
            {
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    for (auto& e : events)
                        if (e.type == type) return true;
                }
                ix::msleep(10);
            }
            return false;
        }

        bool waitForOpen(int timeoutMs = 5000)
        {
            auto deadline = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(timeoutMs);
            while (std::chrono::steady_clock::now() < deadline)
            {
                if (ready) return true;
                ix::msleep(10);
            }
            return false;
        }
    };

    size_t countEvents(const std::vector<Event>& events, WebSocketMessageType type)
    {
        size_t n = 0;
        for (auto& e : events)
            if (e.type == type) ++n;
        return n;
    }

    const Event* findEvent(const std::vector<Event>& events, WebSocketMessageType type)
    {
        for (auto& e : events)
            if (e.type == type) return &e;
        return nullptr;
    }

    // Read frames coming from the websocket endpoint until its CLOSE frame is
    // seen; other frames (e.g. pong replies to pre-error pings) are recorded.
    std::vector<RawPeer::ParsedFrame> drainUntilClose(RawPeer& peer,
                                                      RawPeer::ParsedFrame& closeFrame)
    {
        std::vector<RawPeer::ParsedFrame> others;
        for (int i = 0; i < 100; ++i)
        {
            RawPeer::ParsedFrame f;
            if (!peer.recvFrame(f)) break;
            if (f.opcode == 8)
            {
                closeFrame = f;
                return others;
            }
            others.push_back(f);
        }
        FAIL("did not receive a CLOSE frame");
        return others;
    }

    uint16_t closeCodeOf(const RawPeer::ParsedFrame& f)
    {
        REQUIRE(f.payload.size() >= 2);
        return ((uint16_t)(uint8_t) f.payload[0] << 8) | (uint8_t) f.payload[1];
    }

    // Frame direction when the raw peer is the *server* talking to an ix client.
    const bool kServerToClientMasked = false;
    // Frame direction when the raw peer is the *client* talking to an ix server.
    const bool kClientToServerMasked = true;

    // Verify common expectations after a decompression failure, regardless of
    // which side or which message type.
    void verifyFailureOutcome(EventLog& log,
                              size_t expectedWireSize,
                              const std::string& sentCloseReason)
    {
        REQUIRE(log.waitFor(WebSocketMessageType::Close, 5000));
        ix::msleep(200); // let any stray, forbidden events arrive

        auto events = log.snapshot();
        auto error = findEvent(events, WebSocketMessageType::Error);
        REQUIRE(error != nullptr);
        REQUIRE(error->decompressionError == true);
        REQUIRE(error->str.empty());
        REQUIRE_FALSE(error->reason.empty());
        REQUIRE(error->wireSize == expectedWireSize);

        REQUIRE(countEvents(events, WebSocketMessageType::Error) == 1);
        REQUIRE(countEvents(events, WebSocketMessageType::Close) == 1);
        // No data message, and no partial body, may be delivered for the
        // failed message.
        REQUIRE(findEvent(events, WebSocketMessageType::Message) == nullptr);

        auto close = findEvent(events, WebSocketMessageType::Close);
        REQUIRE(close->closeCode == WebSocketCloseConstants::kInvalidFramePayloadData);
        REQUIRE(close->closeReason == sentCloseReason);

        REQUIRE(log.sendInErrorCallbackFailed.load());
    }

} // namespace

// ---------------------------------------------------------------------------
// Client side: raw permessage-deflate server feeds bad compressed frames to an
// ix WebSocket client.
// ---------------------------------------------------------------------------

TEST_CASE("decompression_failure_client_single_byte", "[deflate][decompression]")
{
    // A single invalid compressed byte 0x07, both text and binary.
    for (int binary = 0; binary < 2; ++binary)
    {
        RawPeer peer;
        REQUIRE(peer.listen());

        WebSocket ws;
        EventLog log;
        log.ws = &ws;
        ws.setUrl("ws://localhost:" + std::to_string(peer.port()) + "/");
        ws.disableAutomaticReconnection();
        ws.enablePerMessageDeflate();
        ws.setOnMessageCallback(
            [&log](const WebSocketMessagePtr& msg) { log.push(msg); });
        ws.start();

        REQUIRE(peer.acceptWithDeflate("permessage-deflate"));
        REQUIRE(log.waitForOpen());

        int opcode = binary ? 2 : 1;
        REQUIRE(peer.sendFrame(
            opcode, true, true, std::string(1, '\x07'), kServerToClientMasked));

        REQUIRE(log.waitFor(WebSocketMessageType::Error));

        // Frames following the bad one, even within later writes, must be
        // completely ignored: no delivery and no pong reply.
        REQUIRE(peer.sendFrame(
            1, true, false, "after-error", kServerToClientMasked));
        REQUIRE(peer.sendFrame(
            9, true, false, "ping-after", kServerToClientMasked));

        RawPeer::ParsedFrame closeFrame;
        auto others = drainUntilClose(peer, closeFrame);
        REQUIRE(closeCodeOf(closeFrame) ==
                WebSocketCloseConstants::kInvalidFramePayloadData);
        std::string reason = closeFrame.payload.substr(2);
        REQUIRE_FALSE(reason.empty());

        // No pong for the post-error ping.
        for (auto& f : others)
        {
            REQUIRE(f.opcode != 0xa);
        }

        // Echo identical code/reason so the closing handshake completes.
        peer.sendCloseEcho(
            closeCodeOf(closeFrame), reason, kServerToClientMasked);

        verifyFailureOutcome(log, 1, reason);

        auto events = log.snapshot();
        REQUIRE(countEvents(events, WebSocketMessageType::Fragment) == 0);
        REQUIRE(countEvents(events, WebSocketMessageType::Ping) == 0);
        REQUIRE(countEvents(events, WebSocketMessageType::Pong) == 0);

        ws.stop();
    }
}

TEST_CASE("decompression_failure_client_same_batch", "[deflate][decompression]")
{
    // The invalid compressed message is immediately followed, IN THE SAME TCP
    // WRITE (one receive batch), by a legal message and a ping. None of the
    // following frames may be delivered or answered.
    RawPeer peer;
    REQUIRE(peer.listen());

    WebSocket ws;
    EventLog log;
    log.ws = &ws;
    ws.setUrl("ws://localhost:" + std::to_string(peer.port()) + "/");
    ws.disableAutomaticReconnection();
    ws.enablePerMessageDeflate();
    ws.setOnMessageCallback(
        [&log](const WebSocketMessagePtr& msg) { log.push(msg); });
    ws.start();

    REQUIRE(peer.acceptWithDeflate("permessage-deflate"));
    REQUIRE(log.waitForOpen());

    std::string batch;
    batch += RawPeer::frame(
        1, true, true, std::string(1, '\x07'), kServerToClientMasked);
    batch += RawPeer::frame(
        1, true, false, "same-batch-message", kServerToClientMasked);
    batch += RawPeer::frame(
        9, true, false, "same-batch-ping", kServerToClientMasked);
    REQUIRE(peer.sendAll(batch));

    REQUIRE(log.waitFor(WebSocketMessageType::Error));

    RawPeer::ParsedFrame closeFrame;
    auto others = drainUntilClose(peer, closeFrame);
    uint16_t code = closeCodeOf(closeFrame);
    std::string reason = closeFrame.payload.substr(2);
    REQUIRE(code == WebSocketCloseConstants::kInvalidFramePayloadData);
    for (auto& f : others)
        REQUIRE(f.opcode != 0xa); // no pong for the same-batch ping

    peer.sendCloseEcho(code, reason, kServerToClientMasked);

    verifyFailureOutcome(log, 1, reason);

    auto events = log.snapshot();
    REQUIRE(countEvents(events, WebSocketMessageType::Message) == 0);
    REQUIRE(countEvents(events, WebSocketMessageType::Ping) == 0);
    REQUIRE(countEvents(events, WebSocketMessageType::Pong) == 0);

    ws.stop();
}

TEST_CASE("decompression_failure_client_valid_then_invalid", "[deflate][decompression]")
{
    // A stream that first decompresses to a large valid prefix and then hits
    // invalid compressed bytes. The partially decoded body must never be
    // delivered, and wireSize covers the whole compressed payload.
    RawDeflater deflater;
    std::string big(40 * 1024, 'x');
    std::string validPrefix = deflater.compress(big);
    std::string bad = validPrefix + std::string(1, '\x07');

    RawPeer peer;
    REQUIRE(peer.listen());

    WebSocket ws;
    EventLog log;
    log.ws = &ws;
    ws.setUrl("ws://localhost:" + std::to_string(peer.port()) + "/");
    ws.disableAutomaticReconnection();
    ws.enablePerMessageDeflate();
    ws.setOnMessageCallback(
        [&log](const WebSocketMessagePtr& msg) { log.push(msg); });
    ws.start();

    REQUIRE(peer.acceptWithDeflate("permessage-deflate"));
    REQUIRE(log.waitForOpen());

    // Byte-by-byte delivery: both the header and payload are split across many
    // socket reads and must not cause a false positive.
    peer.sendByteByByte(
        RawPeer::frame(1, true, true, bad, kServerToClientMasked));

    REQUIRE(log.waitFor(WebSocketMessageType::Error));

    // A legal message right after the bad one (same connection, later bytes).
    REQUIRE(peer.sendFrame(
        1, true, false, "after", kServerToClientMasked));

    RawPeer::ParsedFrame closeFrame;
    auto others = drainUntilClose(peer, closeFrame);
    for (auto& f : others)
        REQUIRE(f.opcode != 0xa);
    uint16_t code = closeCodeOf(closeFrame);
    std::string reason = closeFrame.payload.substr(2);

    // Do NOT echo the close: the existing close wait deadline must end the
    // connection, still emitting exactly one Close with our code/reason.
    (void) code;

    verifyFailureOutcome(log, bad.size(), reason);

    ws.stop();
}

TEST_CASE("decompression_failure_client_fragmented", "[deflate][decompression]")
{
    // Compressed payload spread over several continuation frames, with normal
    // Ping/Pong control frames interleaved before the failure. wireSize is the
    // sum of every fragment payload (frame headers excluded).
    RawDeflater deflater;
    std::string big(50 * 1024, 'y');
    std::string valid = deflater.compress(big);
    std::string bad = valid + std::string(1, '\x07');

    size_t third = bad.size() / 3;
    std::string p1 = bad.substr(0, third);
    std::string p2 = bad.substr(third, third);
    std::string p3 = bad.substr(2 * third);
    REQUIRE(p1.size() + p2.size() + p3.size() == bad.size());

    RawPeer peer;
    REQUIRE(peer.listen());

    WebSocket ws;
    EventLog log;
    log.ws = &ws;
    ws.setUrl("ws://localhost:" + std::to_string(peer.port()) + "/");
    ws.disableAutomaticReconnection();
    ws.enablePerMessageDeflate();
    ws.setOnMessageCallback(
        [&log](const WebSocketMessagePtr& msg) { log.push(msg); });
    ws.start();

    REQUIRE(peer.acceptWithDeflate("permessage-deflate"));
    REQUIRE(log.waitForOpen());

    // First (compressed) fragment ...
    REQUIRE(peer.sendFrame(1, false, true, p1, kServerToClientMasked));
    // ... legal control frames in between are handled as usual ...
    REQUIRE(peer.sendFrame(9, true, false, "mid-ping", kServerToClientMasked));
    REQUIRE(peer.sendFrame(0xa, true, false, "mid-pong", kServerToClientMasked));
    // ... two more continuations, the last one finishing the bad stream.
    REQUIRE(peer.sendFrame(0, false, false, p2, kServerToClientMasked));
    REQUIRE(peer.sendFrame(0, true, false, p3, kServerToClientMasked));

    REQUIRE(log.waitFor(WebSocketMessageType::Error));

    // A ping after the failure must neither be delivered nor answered.
    REQUIRE(peer.sendFrame(9, true, false, "late-ping", kServerToClientMasked));
    REQUIRE(peer.sendFrame(1, true, false, "late-msg", kServerToClientMasked));

    RawPeer::ParsedFrame closeFrame;
    auto others = drainUntilClose(peer, closeFrame);
    uint16_t code = closeCodeOf(closeFrame);
    std::string reason = closeFrame.payload.substr(2);
    REQUIRE(code == WebSocketCloseConstants::kInvalidFramePayloadData);

    // The pre-error ping got exactly one pong; the late ping got none.
    int pongs = 0;
    for (auto& f : others)
    {
        if (f.opcode == 0xa)
        {
            ++pongs;
            REQUIRE(f.payload == "mid-ping");
        }
    }
    REQUIRE(pongs == 1);

    peer.sendCloseEcho(code, reason, kServerToClientMasked);

    verifyFailureOutcome(log, bad.size(), reason);

    auto events = log.snapshot();
    // Body-less fragment notifications before completion are preserved.
    REQUIRE(countEvents(events, WebSocketMessageType::Fragment) == 2);
    // Pre-error control frames were delivered normally...
    REQUIRE(findEvent(events, WebSocketMessageType::Ping) != nullptr);
    REQUIRE(findEvent(events, WebSocketMessageType::Pong) != nullptr);
    // ...but every post-error event is suppressed.
    for (auto& e : events)
    {
        if (e.type == WebSocketMessageType::Ping) REQUIRE(e.str != "late-ping");
    }

    ws.stop();
}

// ---------------------------------------------------------------------------
// Valid compressed messages keep working on the client side.
// ---------------------------------------------------------------------------

TEST_CASE("decompression_valid_messages_client", "[deflate][decompression]")
{
    RawPeer peer;
    REQUIRE(peer.listen());

    WebSocket ws;
    EventLog log;
    ws.setUrl("ws://localhost:" + std::to_string(peer.port()) + "/");
    ws.disableAutomaticReconnection();
    ws.enablePerMessageDeflate();
    ws.setOnMessageCallback(
        [&log](const WebSocketMessagePtr& msg) { log.push(msg); });
    ws.start();

    REQUIRE(peer.acceptWithDeflate("permessage-deflate"));
    REQUIRE(log.waitForOpen());

    RawDeflater deflater;

    // 1. Empty compressed message (special 0x02 0x00 encoding).
    REQUIRE(peer.sendFrame(
        1, true, true, std::string("\x02\x00", 2), kServerToClientMasked));

    // 2. Message larger than 32KiB, single (64-bit length) frame.
    std::string big(70 * 1024, 'z');
    REQUIRE(peer.sendFrame(
        1, true, true, deflater.compress(big), kServerToClientMasked));

    // 3. Same large message split across multiple continuation frames.
    std::string compressed = deflater.compress(big);
    size_t half = compressed.size() / 2;
    REQUIRE(peer.sendFrame(
        2, false, true, compressed.substr(0, half), kServerToClientMasked));
    REQUIRE(peer.sendFrame(
        0, true, false, compressed.substr(half), kServerToClientMasked));

    // 4. Consecutive compressed messages produced with context takeover.
    std::string a = "the quick brown fox jumps over the lazy dog";
    std::string b = "the quick brown fox jumps over the lazy dog (again)";
    REQUIRE(peer.sendFrame(
        1, true, true, deflater.compress(a), kServerToClientMasked));
    REQUIRE(peer.sendFrame(
        1, true, true, deflater.compress(b), kServerToClientMasked));

    // Wait for all 5 messages (empty, big, big-binary-fragmented, a, b).
    bool delivered = false;
    for (int i = 0; i < 500; ++i)
    {
        auto ev = log.snapshot();
        if (countEvents(ev, WebSocketMessageType::Message) >= 5)
        {
            delivered = true;
            break;
        }
        ix::msleep(10);
    }
    REQUIRE(delivered);

    auto events = log.snapshot();
    auto messages = std::vector<Event>();
    for (auto& e : events)
        if (e.type == WebSocketMessageType::Message) messages.push_back(e);
    REQUIRE(messages.size() == 5);

    REQUIRE(messages[0].str == "");
    REQUIRE(messages[0].binary == false);
    REQUIRE(messages[1].str == big);
    REQUIRE(messages[1].binary == false);
    REQUIRE(messages[2].str == big);
    REQUIRE(messages[2].binary == true);
    REQUIRE(messages[3].str == a);
    REQUIRE(messages[4].str == b);

    for (auto& e : events)
    {
        REQUIRE(e.type != WebSocketMessageType::Error);
        REQUIRE(e.type != WebSocketMessageType::Close);
    }

    ws.stop();
}

TEST_CASE("decompression_valid_before_error_client", "[deflate][decompression]")
{
    // A fully received valid message before the bad one is still delivered.
    RawDeflater deflater;
    std::string good = deflater.compress("perfectly valid message");

    RawPeer peer;
    REQUIRE(peer.listen());

    WebSocket ws;
    EventLog log;
    log.ws = &ws;
    ws.setUrl("ws://localhost:" + std::to_string(peer.port()) + "/");
    ws.disableAutomaticReconnection();
    ws.enablePerMessageDeflate();
    ws.setOnMessageCallback(
        [&log](const WebSocketMessagePtr& msg) { log.push(msg); });
    ws.start();

    REQUIRE(peer.acceptWithDeflate("permessage-deflate"));
    REQUIRE(log.waitForOpen());

    REQUIRE(peer.sendFrame(1, true, true, good, kServerToClientMasked));

    // Wait for the good message to be delivered.
    bool gotGood = false;
    for (int i = 0; i < 200; ++i)
    {
        auto ev = log.snapshot();
        if (findEvent(ev, WebSocketMessageType::Message))
        {
            gotGood = true;
            break;
        }
        ix::msleep(10);
    }
    REQUIRE(gotGood);

    REQUIRE(peer.sendFrame(
        1, true, true, std::string(1, '\x07'), kServerToClientMasked));
    REQUIRE(peer.sendFrame(
        1, true, true, good, kServerToClientMasked)); // must not be delivered

    RawPeer::ParsedFrame closeFrame;
    drainUntilClose(peer, closeFrame);
    uint16_t code = closeCodeOf(closeFrame);
    std::string reason = closeFrame.payload.substr(2);
    peer.sendCloseEcho(code, reason, kServerToClientMasked);

    REQUIRE(log.waitFor(WebSocketMessageType::Close));
    ix::msleep(200);

    auto events = log.snapshot();
    REQUIRE(countEvents(events, WebSocketMessageType::Message) == 1);
    auto msg = findEvent(events, WebSocketMessageType::Message);
    REQUIRE(msg->str == "perfectly valid message");
    REQUIRE(countEvents(events, WebSocketMessageType::Error) == 1);
    REQUIRE(countEvents(events, WebSocketMessageType::Close) == 1);

    ws.stop();
}

// ---------------------------------------------------------------------------
// Server side: raw permessage-deflate client sends a bad compressed frame to
// an ix WebSocketServer.
// ---------------------------------------------------------------------------

TEST_CASE("decompression_failure_server", "[deflate][decompression]")
{
    for (int binary = 0; binary < 2; ++binary)
    {
        int port = getFreePort();
        WebSocketServer server(port);
        EventLog log;

        server.setOnClientMessageCallback(
            [&log, &server](std::shared_ptr<ConnectionState> state,
                            WebSocket& webSocket,
                            const WebSocketMessagePtr& msg) {
                (void) state;
                (void) server;
                log.ws = &webSocket;
                log.push(msg);
            });

        auto res = server.listen();
        REQUIRE(res.first);
        server.start();

        RawPeer peer;
        REQUIRE(peer.connect(port));

        REQUIRE(log.waitForOpen());

        int opcode = binary ? 2 : 1;
        REQUIRE(peer.sendFrame(
            opcode, true, true, std::string(1, '\x07'), kClientToServerMasked));
        // Post-error ping/message must be ignored by the server endpoint.
        REQUIRE(peer.sendFrame(
            9, true, false, "ping-after", kClientToServerMasked));
        REQUIRE(peer.sendFrame(
            1, true, false, "after-error", kClientToServerMasked));

        REQUIRE(log.waitFor(WebSocketMessageType::Error));

        RawPeer::ParsedFrame closeFrame;
        auto others = drainUntilClose(peer, closeFrame);
        uint16_t code = closeCodeOf(closeFrame);
        std::string reason = closeFrame.payload.substr(2);
        REQUIRE(code == WebSocketCloseConstants::kInvalidFramePayloadData);
        REQUIRE_FALSE(reason.empty());
        for (auto& f : others)
            REQUIRE(f.opcode != 0xa); // no pong for post-error ping

        // The raw client echoes the close to finish the handshake.
        peer.sendCloseEcho(code, reason, kClientToServerMasked);

        verifyFailureOutcome(log, 1, reason);

        auto events = log.snapshot();
        REQUIRE(findEvent(events, WebSocketMessageType::Message) == nullptr);
        REQUIRE(countEvents(events, WebSocketMessageType::Ping) == 0);

        ix::msleep(200);
        server.stop();
    }
}

// ---------------------------------------------------------------------------
// A failure only affects the current connection: reconnecting the same client
// WebSocket object works and carries no leftover state.
// ---------------------------------------------------------------------------

TEST_CASE("decompression_failure_then_reconnect_client", "[deflate][decompression]")
{
    RawPeer peer;
    REQUIRE(peer.listen());

    WebSocket ws;
    EventLog log;
    log.ws = &ws;
    ws.setUrl("ws://localhost:" + std::to_string(peer.port()) + "/");
    ws.enablePerMessageDeflate(); // automatic reconnection stays enabled
    ws.setOnMessageCallback(
        [&log](const WebSocketMessagePtr& msg) { log.push(msg); });
    ws.start();

    // --- First connection: bad payload ---
    REQUIRE(peer.acceptWithDeflate("permessage-deflate"));
    REQUIRE(log.waitForOpen());
    REQUIRE(peer.sendFrame(
        1, true, true, std::string(1, '\x07'), kServerToClientMasked));

    RawPeer::ParsedFrame closeFrame1;
    drainUntilClose(peer, closeFrame1);
    std::string reason1 = closeFrame1.payload.substr(2);
    peer.sendCloseEcho(
        closeCodeOf(closeFrame1), reason1, kServerToClientMasked);
    peer.closePeer();

    REQUIRE(log.waitFor(WebSocketMessageType::Close));

    // --- Automatic reconnect on the same object ---
    REQUIRE(peer.acceptWithDeflate("permessage-deflate"));

    // Wait for the second Open.
    int opens = 0;
    for (int i = 0; i < 500; ++i)
    {
        auto ev = log.snapshot();
        opens = (int) countEvents(ev, WebSocketMessageType::Open);
        if (opens >= 2) break;
        ix::msleep(10);
    }
    REQUIRE(opens >= 2);

    // A valid compressed message on the fresh connection is received whole,
    // with no residue from the previous failure.
    RawDeflater deflater;
    std::string body = "fresh connection, fresh state";
    REQUIRE(peer.sendFrame(
        1, true, true, deflater.compress(body), kServerToClientMasked));

    bool gotBody = false;
    for (int i = 0; i < 300; ++i)
    {
        auto ev = log.snapshot();
        for (auto& e : ev)
        {
            if (e.type == WebSocketMessageType::Message && e.str == body)
            {
                gotBody = true;
            }
        }
        if (gotBody) break;
        ix::msleep(10);
    }
    REQUIRE(gotBody);

    ws.stop();
}
