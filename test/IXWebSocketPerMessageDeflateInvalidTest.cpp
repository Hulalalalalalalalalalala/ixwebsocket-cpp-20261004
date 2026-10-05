/*
 *  IXWebSocketPerMessageDeflateInvalidTest.cpp
 *
 *  An invalid compressed payload (permessage-deflate) must fail the whole
 *  message: an Error is emitted (no Message, no partial body) and the
 *  connection is closed with close code 1007.
 *
 *  Raw socket peers are used so that exact, invalid, wire bytes can be sent.
 *
 *  make build_test && build/test/IXWebSocketPerMessageDeflateInvalidTest
 */

#include "IXTest.h"
#include <catch_amalgamated.hpp>
#include <ixwebsocket/IXWebSocket.h>
#include <ixwebsocket/IXWebSocketCloseConstants.h>
#include <ixwebsocket/IXWebSocketHandshakeKeyGen.h>
#include <ixwebsocket/IXWebSocketPerMessageDeflateCodec.h>
#include <ixwebsocket/IXWebSocketServer.h>
#include <atomic>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace ix;

namespace
{
    const uint16_t kCloseCodeInvalidFramePayloadData = 1007;

    //
    // Socket helpers
    //
    void setRecvTimeout(int fd, int seconds)
    {
        struct timeval tv;
        tv.tv_sec = seconds;
        tv.tv_usec = 0;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }

    int makeListenSocket(int port)
    {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = htons(port);

        if (bind(fd, (struct sockaddr*) &addr, sizeof(addr)) != 0) return -1;
        if (listen(fd, 4) != 0) return -1;
        return fd;
    }

    int connectTo(int port)
    {
        int fd = socket(AF_INET, SOCK_STREAM, 0);

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = htons(port);

        if (connect(fd, (struct sockaddr*) &addr, sizeof(addr)) != 0)
        {
            close(fd);
            return -1;
        }
        return fd;
    }

    bool sendAll(int fd, const std::string& data)
    {
        size_t sent = 0;
        while (sent < data.size())
        {
            ssize_t ret = send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
            if (ret <= 0) return false;
            sent += (size_t) ret;
        }
        return true;
    }

    bool readExact(int fd, char* buf, size_t n)
    {
        size_t got = 0;
        while (got < n)
        {
            ssize_t ret = recv(fd, buf + got, n - got, 0);
            if (ret <= 0) return false;
            got += (size_t) ret;
        }
        return true;
    }

    bool readHttpHeaders(int fd, std::string& out)
    {
        out.clear();
        char c;
        while (out.find("\r\n\r\n") == std::string::npos)
        {
            ssize_t ret = recv(fd, &c, 1, 0);
            if (ret <= 0) return false;
            out.push_back(c);
            if (out.size() > 1 << 16) return false;
        }
        return true;
    }

    std::string extractHeader(const std::string& request, const std::string& name)
    {
        std::string lower;
        lower.resize(request.size());
        for (size_t i = 0; i < request.size(); ++i)
        {
            lower[i] = (char) tolower(request[i]);
        }

        std::string key = name + ":";
        auto pos = lower.find(key);
        if (pos == std::string::npos) return "";

        pos += key.size();
        while (pos < request.size() && request[pos] == ' ')
            pos++;

        auto end = request.find("\r\n", pos);
        return request.substr(pos, end - pos);
    }

    //
    // WebSocket frame helpers
    //
    struct Frame
    {
        bool fin = false;
        bool rsv1 = false;
        uint8_t opcode = 0;
        std::string payload;
    };

    std::string encodeFrame(uint8_t opcode,
                            bool fin,
                            bool rsv1,
                            const std::string& payload,
                            bool masked)
    {
        std::string out;

        uint8_t b0 = opcode & 0x0f;
        if (fin) b0 |= 0x80;
        if (rsv1) b0 |= 0x40;
        out.push_back((char) b0);

        uint8_t maskBit = masked ? 0x80 : 0;
        size_t n = payload.size();
        if (n < 126)
        {
            out.push_back((char) (maskBit | n));
        }
        else if (n < 65536)
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
        if (masked)
        {
            out.append((char*) key, 4);
        }

        for (size_t i = 0; i < n; ++i)
        {
            char c = payload[i];
            if (masked) c ^= key[i & 3];
            out.push_back(c);
        }

        return out;
    }

    bool readFrame(int fd, Frame& f)
    {
        char hdr[2];
        if (!readExact(fd, hdr, 2)) return false;

        f.fin = (hdr[0] & 0x80) != 0;
        f.rsv1 = (hdr[0] & 0x40) != 0;
        f.opcode = (uint8_t)(hdr[0] & 0x0f);

        bool masked = (hdr[1] & 0x80) != 0;
        uint64_t n = (uint64_t)(hdr[1] & 0x7f);
        if (n == 126)
        {
            char e[2];
            if (!readExact(fd, e, 2)) return false;
            n = ((uint64_t)(uint8_t) e[0] << 8) | (uint8_t) e[1];
        }
        else if (n == 127)
        {
            char e[8];
            if (!readExact(fd, e, 8)) return false;
            n = 0;
            for (int i = 0; i < 8; ++i)
            {
                n = (n << 8) | (uint8_t) e[i];
            }
        }

        uint8_t key[4] = {};
        if (masked && !readExact(fd, (char*) key, 4)) return false;

        f.payload.assign((size_t) n, 0);
        if (n > 0 && !readExact(fd, &f.payload[0], (size_t) n)) return false;

        if (masked)
        {
            for (uint64_t i = 0; i < n; ++i)
            {
                f.payload[i] ^= key[i & 3];
            }
        }

        return true;
    }

    bool waitFor(const std::function<bool()>& pred, int timeoutMs)
    {
        int waited = 0;
        while (waited < timeoutMs)
        {
            if (pred()) return true;
            ix::msleep(10);
            waited += 10;
        }
        return pred();
    }

    std::string makeBigPayload()
    {
        // Pseudo random (hardly compressible) payload, larger than the 32KiB
        // internal read chunk size, so that the frame is read in several goes.
        std::string out;
        out.resize(40000);
        uint32_t x = 12345;
        for (size_t i = 0; i < out.size(); ++i)
        {
            x = x * 1103515245 + 12345;
            out[i] = (char) (x >> 16);
        }
        return out;
    }

    //
    // What the raw peer observes back from the ix endpoint
    //
    struct PeerObservation
    {
        std::vector<size_t> validCompressedSizes; // compressed size of each valid message sent
        size_t invalidPayloadSize = 0;
        int pongCount = 0;
        int unexpectedFrameCount = 0;
        bool gotClose = false;
        uint16_t closeCode = 0;
        std::string closeReason;
        std::string harnessError;
    };

    //
    // Events recorded on the ix side under test
    //
    struct RecordedEvents
    {
        std::mutex mutex;
        int openCount = 0;
        std::vector<std::string> messages;
        std::vector<bool> messageBinary;
        std::vector<size_t> messageWireSizes;
        int fragmentCount = 0;
        int pingCount = 0;
        int pongCount = 0;
        int errorCount = 0;
        bool errorDecompression = false;
        bool errorStrEmpty = false;
        std::string errorReason;
        size_t errorWireSize = 0;
        ix::ReadyState readyStateAtError = ix::ReadyState::Open;
        bool sendSucceededAtError = true;
        int closeCount = 0;
        uint16_t closeCode = 0;
        std::string closeReason;
        bool closeRemote = true;
    };

    void recordEvent(ix::WebSocket& ws, RecordedEvents& ev, const ix::WebSocketMessagePtr& msg)
    {
        if (msg->type == ix::WebSocketMessageType::Error)
        {
            // The connection must already be closing, and sending must fail.
            ix::ReadyState state = ws.getReadyState();
            bool sendOk = ws.sendText("should fail").success;

            std::lock_guard<std::mutex> lock(ev.mutex);
            ev.errorCount++;
            ev.errorDecompression = msg->errorInfo.decompressionError;
            ev.errorStrEmpty = msg->str.empty();
            ev.errorReason = msg->errorInfo.reason;
            ev.errorWireSize = msg->wireSize;
            ev.readyStateAtError = state;
            ev.sendSucceededAtError = sendOk;
            return;
        }

        std::lock_guard<std::mutex> lock(ev.mutex);
        switch (msg->type)
        {
            case ix::WebSocketMessageType::Open: ev.openCount++; break;
            case ix::WebSocketMessageType::Message:
                ev.messages.push_back(msg->str);
                ev.messageBinary.push_back(msg->binary);
                ev.messageWireSizes.push_back(msg->wireSize);
                break;
            case ix::WebSocketMessageType::Fragment: ev.fragmentCount++; break;
            case ix::WebSocketMessageType::Ping: ev.pingCount++; break;
            case ix::WebSocketMessageType::Pong: ev.pongCount++; break;
            case ix::WebSocketMessageType::Close:
                ev.closeCount++;
                ev.closeCode = msg->closeInfo.code;
                ev.closeReason = msg->closeInfo.reason;
                ev.closeRemote = msg->closeInfo.remote;
                break;
            default: break;
        }
    }

    struct ClientScenarioOptions
    {
        uint8_t invalidOpcode = 0x1; // text
        std::string invalidPayload = std::string("\x07", 1);
        bool fragmentInvalid = false; // split the invalid payload in 2 frames, ping in between
        int delayAfterInvalidMs = 0;  // delay before sending trailing valid traffic
        bool echoClose = true;        // whether to answer the CLOSE frame back
    };

    //
    // A raw WebSocket server which sends valid compressed messages, then an
    // invalid compressed message, then more (valid) traffic which must be
    // dropped by the client. It then expects a CLOSE frame with code 1007.
    //
    void rawServerMain(int listenFd,
                       ClientScenarioOptions opts,
                       std::string bigPayload,
                       std::shared_ptr<PeerObservation> obs)
    {
        int fd = accept(listenFd, nullptr, nullptr);
        if (fd < 0)
        {
            obs->harnessError = "accept failed";
            return;
        }
        setRecvTimeout(fd, 15);

        std::string request;
        if (!readHttpHeaders(fd, request))
        {
            obs->harnessError = "reading http request failed";
            close(fd);
            return;
        }

        std::string key = extractHeader(request, "sec-websocket-key");
        char output[29] = {};
        WebSocketHandshakeKeyGen::generate(key, output);

        std::string response = "HTTP/1.1 101 Switching Protocols\r\n"
                               "Upgrade: websocket\r\n"
                               "Connection: Upgrade\r\n"
                               "Sec-WebSocket-Accept: " +
                               std::string(output) +
                               "\r\n"
                               "Sec-WebSocket-Extensions: permessage-deflate\r\n"
                               "\r\n";
        if (!sendAll(fd, response))
        {
            obs->harnessError = "sending handshake response failed";
            close(fd);
            return;
        }

        // Keep the compression context between messages (context takeover)
        WebSocketPerMessageDeflateCompressor compressor;
        compressor.init(15, false);

        auto sendValid = [&](uint8_t opcode, const std::string& data) {
            std::string compressed;
            compressor.compress(data, compressed);
            obs->validCompressedSizes.push_back(compressed.size());
            sendAll(fd, encodeFrame(opcode, true, true, compressed, false));
        };

        // A few valid compressed messages: text, empty, large binary,
        // repeated text (context takeover) ...
        sendValid(0x1, "hello");
        sendValid(0x1, "");
        sendValid(0x2, bigPayload);
        sendValid(0x1, "hello");

        // ... and a fragmented one, with a ping interleaved between fragments
        {
            std::string compressed;
            compressor.compress(std::string("fragmented valid payload"), compressed);
            obs->validCompressedSizes.push_back(compressed.size());

            size_t cut = 3;
            sendAll(fd, encodeFrame(0x1, false, true, compressed.substr(0, cut), false));
            sendAll(fd, encodeFrame(0x9, true, false, "p1", false));
            sendAll(fd, encodeFrame(0x0, true, false, compressed.substr(cut), false));
        }

        // The invalid message
        obs->invalidPayloadSize = opts.invalidPayload.size();
        if (opts.fragmentInvalid)
        {
            size_t cut = opts.invalidPayload.size() / 2;
            sendAll(fd, encodeFrame(opts.invalidOpcode, false, true,
                                    opts.invalidPayload.substr(0, cut), false));
            sendAll(fd, encodeFrame(0x9, true, false, "p2", false));
            sendAll(fd, encodeFrame(0x0, true, false, opts.invalidPayload.substr(cut), false));
        }
        else
        {
            sendAll(fd, encodeFrame(opts.invalidOpcode, true, true, opts.invalidPayload, false));
        }

        if (opts.delayAfterInvalidMs > 0)
        {
            ix::msleep(opts.delayAfterInvalidMs);
        }

        // Trailing traffic, must be dropped (no message, no pong reply)
        {
            std::string compressed;
            compressor.compress(std::string("world"), compressed);
            sendAll(fd, encodeFrame(0x1, true, true, compressed, false));
            sendAll(fd, encodeFrame(0x9, true, false, "p3", false));
        }

        // Read frames until the CLOSE one
        Frame f;
        while (readFrame(fd, f))
        {
            if (f.opcode == 0x8)
            {
                obs->gotClose = true;
                if (f.payload.size() >= 2)
                {
                    obs->closeCode =
                        (uint16_t)(((uint16_t)(uint8_t) f.payload[0] << 8) |
                                   (uint8_t) f.payload[1]);
                    obs->closeReason = f.payload.substr(2);
                }
                if (opts.echoClose)
                {
                    // Echo the close frame back to complete the closing handshake
                    sendAll(fd, encodeFrame(0x8, true, false, f.payload, false));
                    break;
                }
                // Otherwise never answer: the client must give up waiting
                // after its closing delay and close the connection anyway.
            }
            else if (f.opcode == 0xA)
            {
                obs->pongCount++;
            }
            else
            {
                obs->unexpectedFrameCount++;
            }
        }

        close(fd);
    }

    // A raw server which only sends valid compressed messages, then a normal
    // close. Used to check that a WebSocket object can reconnect cleanly
    // after a previous connection failed on a decompression error.
    void rawServerValidOnlyMain(int listenFd,
                                const std::string& text,
                                std::shared_ptr<PeerObservation> obs)
    {
        int fd = accept(listenFd, nullptr, nullptr);
        if (fd < 0)
        {
            obs->harnessError = "accept failed";
            return;
        }
        setRecvTimeout(fd, 15);

        std::string request;
        if (!readHttpHeaders(fd, request))
        {
            obs->harnessError = "reading http request failed";
            close(fd);
            return;
        }

        std::string key = extractHeader(request, "sec-websocket-key");
        char output[29] = {};
        WebSocketHandshakeKeyGen::generate(key, output);

        std::string response = "HTTP/1.1 101 Switching Protocols\r\n"
                               "Upgrade: websocket\r\n"
                               "Connection: Upgrade\r\n"
                               "Sec-WebSocket-Accept: " +
                               std::string(output) +
                               "\r\n"
                               "Sec-WebSocket-Extensions: permessage-deflate\r\n"
                               "\r\n";
        if (!sendAll(fd, response))
        {
            obs->harnessError = "sending handshake response failed";
            close(fd);
            return;
        }

        WebSocketPerMessageDeflateCompressor compressor;
        compressor.init(15, false);

        std::string compressed;
        compressor.compress(text, compressed);
        obs->validCompressedSizes.push_back(compressed.size());
        sendAll(fd, encodeFrame(0x1, true, true, compressed, false));

        ix::msleep(200);

        // Normal closure
        std::string closePayload;
        closePayload.push_back((char) (1000 >> 8));
        closePayload.push_back((char) (1000 & 0xff));
        sendAll(fd, encodeFrame(0x8, true, false, closePayload, false));

        Frame f;
        while (readFrame(fd, f))
        {
            if (f.opcode == 0x8)
            {
                obs->gotClose = true;
                break;
            }
        }

        close(fd);
    }

    void checkValidPrefix(const RecordedEvents& ev,
                          const PeerObservation& obs,
                          const std::string& bigPayload)
    {
        std::vector<std::string> expectedMessages = {
            "hello", "", bigPayload, "hello", "fragmented valid payload"};
        std::vector<bool> expectedBinary = {false, false, true, false, false};

        REQUIRE(ev.messages == expectedMessages);
        REQUIRE(ev.messageBinary == expectedBinary);
        REQUIRE(ev.messageWireSizes == obs.validCompressedSizes);
    }

    void checkFailure(const RecordedEvents& ev,
                      const PeerObservation& obs,
                      int expectedFragments,
                      int expectedPings)
    {
        // One Error, no Message for the invalid payload
        REQUIRE(ev.errorCount == 1);
        REQUIRE(ev.errorDecompression);
        REQUIRE(ev.errorStrEmpty);
        REQUIRE(ev.errorReason == WebSocketCloseConstants::kDecompressionErrorMessage);
        REQUIRE(ev.errorWireSize == obs.invalidPayloadSize);

        // The connection was already closing inside the error callback
        REQUIRE(ev.readyStateAtError == ix::ReadyState::Closing);
        REQUIRE(!ev.sendSucceededAtError);

        // Fragment / ping events before the error are still delivered,
        // nothing after the error
        REQUIRE(ev.fragmentCount == expectedFragments);
        REQUIRE(ev.pingCount == expectedPings);
        REQUIRE(ev.pongCount == 0);

        // A single Close, with the failure code and reason
        REQUIRE(ev.closeCount == 1);
        REQUIRE(ev.closeCode == kCloseCodeInvalidFramePayloadData);
        REQUIRE(ev.closeReason == WebSocketCloseConstants::kDecompressionErrorMessage);
        REQUIRE(!ev.closeRemote);

        // The peer received the close frame with code 1007, and no pong for
        // the ping sent after the error
        REQUIRE(obs.harnessError.empty());
        REQUIRE(obs.gotClose);
        REQUIRE(obs.closeCode == kCloseCodeInvalidFramePayloadData);
        REQUIRE(obs.closeReason == WebSocketCloseConstants::kDecompressionErrorMessage);
        REQUIRE(obs.pongCount == expectedPings);
        REQUIRE(obs.unexpectedFrameCount == 0);
    }

    void runClientScenario(const ClientScenarioOptions& opts,
                           int expectedFragments,
                           int expectedPings)
    {
        int port = getFreePort();
        int listenFd = makeListenSocket(port);
        REQUIRE(listenFd >= 0);

        auto obs = std::make_shared<PeerObservation>();
        std::string bigPayload = makeBigPayload();
        std::thread serverThread(rawServerMain, listenFd, opts, bigPayload, obs);

        RecordedEvents ev;
        ix::WebSocket ws;
        {
            std::stringstream ss;
            ss << "ws://localhost:" << port << "/";
            ws.setUrl(ss.str());
        }
        ws.enablePerMessageDeflate();
        ws.disableAutomaticReconnection();
        ws.setOnMessageCallback(
            [&ws, &ev](const ix::WebSocketMessagePtr& msg) { recordEvent(ws, ev, msg); });
        ws.start();

        REQUIRE(waitFor([&] {
            std::lock_guard<std::mutex> lock(ev.mutex);
            return ev.closeCount >= 1;
        },
                        15000));

        ws.stop();
        serverThread.join();
        close(listenFd);

        checkValidPrefix(ev, *obs, bigPayload);
        checkFailure(ev, *obs, expectedFragments, expectedPings);
    }
} // namespace

TEST_CASE("per-message-deflate-invalid-client", "[zlib][pmd-invalid]")
{
    SECTION("invalid compressed text payload fails the message and closes the connection")
    {
        ClientScenarioOptions opts;
        runClientScenario(opts, 1, 1);
    }

    SECTION("invalid compressed binary payload fails the message and closes the connection")
    {
        ClientScenarioOptions opts;
        opts.invalidOpcode = 0x2;
        opts.delayAfterInvalidMs = 300;
        runClientScenario(opts, 1, 1);
    }

    SECTION("valid content followed by invalid compressed data fails the whole message")
    {
        ClientScenarioOptions opts;
        // A valid compressed prefix, then garbage: the partial output must
        // not be delivered either.
        WebSocketPerMessageDeflateCompressor compressor;
        compressor.init(15, false);
        std::string compressed;
        compressor.compress(std::string("valid prefix that must be lost"), compressed);
        opts.invalidPayload = compressed + "\x07";
        runClientScenario(opts, 1, 1);
    }

    SECTION("fragmented invalid message, with a ping interleaved between fragments")
    {
        ClientScenarioOptions opts;
        WebSocketPerMessageDeflateCompressor compressor;
        compressor.init(15, false);
        std::string compressed;
        compressor.compress(std::string("fragmented content that must be lost"), compressed);
        opts.invalidPayload = compressed + "\x07";
        opts.fragmentInvalid = true;
        opts.delayAfterInvalidMs = 300;
        // One fragment + one ping for the valid fragmented message,
        // one more fragment + one more ping for the invalid one.
        runClientScenario(opts, 2, 2);
    }

    SECTION("no close answer from the peer: the closing delay still terminates the connection")
    {
        ClientScenarioOptions opts;
        opts.echoClose = false;
        runClientScenario(opts, 1, 1);
    }
}

TEST_CASE("per-message-deflate-invalid-reconnect", "[zlib][pmd-invalid]")
{
    SECTION("the same WebSocket object can reconnect and receive messages after a failure")
    {
        // First connection: invalid payload
        int port = getFreePort();
        int listenFd = makeListenSocket(port);
        REQUIRE(listenFd >= 0);

        auto obs = std::make_shared<PeerObservation>();
        ClientScenarioOptions opts;
        std::string bigPayload = makeBigPayload();
        std::thread serverThread(rawServerMain, listenFd, opts, bigPayload, obs);

        RecordedEvents ev;
        ix::WebSocket ws;
        {
            std::stringstream ss;
            ss << "ws://localhost:" << port << "/";
            ws.setUrl(ss.str());
        }
        ws.enablePerMessageDeflate();
        ws.disableAutomaticReconnection();
        ws.setOnMessageCallback(
            [&ws, &ev](const ix::WebSocketMessagePtr& msg) { recordEvent(ws, ev, msg); });
        ws.start();

        REQUIRE(waitFor([&] {
            std::lock_guard<std::mutex> lock(ev.mutex);
            return ev.closeCount >= 1;
        },
                        15000));
        ws.stop();
        serverThread.join();
        close(listenFd);

        REQUIRE(ev.errorCount == 1);
        REQUIRE(ev.errorDecompression);
        REQUIRE(ev.closeCode == kCloseCodeInvalidFramePayloadData);

        // Second connection on the same WebSocket object: valid traffic only
        RecordedEvents ev2;
        int port2 = getFreePort();
        int listenFd2 = makeListenSocket(port2);
        REQUIRE(listenFd2 >= 0);

        auto obs2 = std::make_shared<PeerObservation>();
        std::thread serverThread2(rawServerValidOnlyMain, listenFd2, "hello again", obs2);

        {
            std::stringstream ss;
            ss << "ws://localhost:" << port2 << "/";
            ws.setUrl(ss.str());
        }
        ws.setOnMessageCallback(
            [&ws, &ev2](const ix::WebSocketMessagePtr& msg) { recordEvent(ws, ev2, msg); });
        ws.start();

        REQUIRE(waitFor([&] {
            std::lock_guard<std::mutex> lock(ev2.mutex);
            return ev2.closeCount >= 1;
        },
                        15000));
        ws.stop();
        serverThread2.join();
        close(listenFd2);

        // No state or partial body is carried over from the failed connection
        REQUIRE(ev2.openCount == 1);
        REQUIRE(ev2.messages == std::vector<std::string>{"hello again"});
        REQUIRE(ev2.messageWireSizes == obs2->validCompressedSizes);
        REQUIRE(ev2.errorCount == 0);
        REQUIRE(ev2.closeCount == 1);
        REQUIRE(ev2.closeCode == 1000);
    }
}

TEST_CASE("per-message-deflate-invalid-server", "[zlib][pmd-invalid]")
{
    SECTION("a server receiving an invalid compressed payload fails the message and closes")
    {
        int port = getFreePort();
        ix::WebSocketServer server(port);

        RecordedEvents ev;
        server.setOnClientMessageCallback(
            [&ev](std::shared_ptr<ConnectionState> /*connectionState*/,
                  WebSocket& webSocket,
                  const ix::WebSocketMessagePtr& msg) {
                recordEvent(webSocket, ev, msg);
            });

        auto res = server.listen();
        REQUIRE(res.first);
        server.start();

        // Raw client talking to the ix server
        int fd = connectTo(port);
        REQUIRE(fd >= 0);
        setRecvTimeout(fd, 15);

        std::string request = "GET / HTTP/1.1\r\n"
                              "Host: localhost\r\n"
                              "Upgrade: websocket\r\n"
                              "Connection: Upgrade\r\n"
                              "Sec-WebSocket-Version: 13\r\n"
                              "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                              "Sec-WebSocket-Extensions: permessage-deflate\r\n"
                              "\r\n";
        REQUIRE(sendAll(fd, request));

        std::string response;
        REQUIRE(readHttpHeaders(fd, response));
        REQUIRE(response.find("101") != std::string::npos);
        REQUIRE(response.find("permessage-deflate") != std::string::npos);

        WebSocketPerMessageDeflateCompressor compressor;
        compressor.init(15, false);

        std::string compressed;
        compressor.compress(std::string("hello"), compressed);
        size_t validCompressedSize = compressed.size();

        // A valid message, then an invalid one (clients must mask)
        REQUIRE(sendAll(fd, encodeFrame(0x1, true, true, compressed, true)));
        REQUIRE(sendAll(fd, encodeFrame(0x1, true, true, std::string("\x07", 1), true)));

        ix::msleep(300);

        // Trailing traffic, must be dropped
        std::string compressedWorld;
        compressor.compress(std::string("world"), compressedWorld);
        REQUIRE(sendAll(fd, encodeFrame(0x1, true, true, compressedWorld, true)));
        REQUIRE(sendAll(fd, encodeFrame(0x9, true, false, "p3", true)));

        // The server must close with 1007 and not reply to the ping
        PeerObservation obs;
        Frame f;
        while (readFrame(fd, f))
        {
            if (f.opcode == 0x8)
            {
                obs.gotClose = true;
                if (f.payload.size() >= 2)
                {
                    obs.closeCode =
                        (uint16_t)(((uint16_t)(uint8_t) f.payload[0] << 8) |
                                   (uint8_t) f.payload[1]);
                    obs.closeReason = f.payload.substr(2);
                }
                // Echo the close frame back
                REQUIRE(sendAll(fd, encodeFrame(0x8, true, false, f.payload, true)));
                break;
            }
            else if (f.opcode == 0xA)
            {
                obs.pongCount++;
            }
            else
            {
                obs.unexpectedFrameCount++;
            }
        }
        close(fd);

        REQUIRE(waitFor([&] {
            std::lock_guard<std::mutex> lock(ev.mutex);
            return ev.closeCount >= 1;
        },
                        15000));

        server.stop();

        // The valid message was delivered, the invalid one was not
        REQUIRE(ev.messages == std::vector<std::string>{"hello"});
        REQUIRE(ev.messageWireSizes == std::vector<size_t>{validCompressedSize});

        REQUIRE(ev.errorCount == 1);
        REQUIRE(ev.errorDecompression);
        REQUIRE(ev.errorStrEmpty);
        REQUIRE(ev.errorReason == WebSocketCloseConstants::kDecompressionErrorMessage);
        REQUIRE(ev.errorWireSize == 1);
        REQUIRE(ev.readyStateAtError == ix::ReadyState::Closing);
        REQUIRE(!ev.sendSucceededAtError);

        REQUIRE(ev.pingCount == 0);
        REQUIRE(ev.closeCount == 1);
        REQUIRE(ev.closeCode == kCloseCodeInvalidFramePayloadData);
        REQUIRE(ev.closeReason == WebSocketCloseConstants::kDecompressionErrorMessage);
        REQUIRE(!ev.closeRemote);

        REQUIRE(obs.gotClose);
        REQUIRE(obs.closeCode == kCloseCodeInvalidFramePayloadData);
        REQUIRE(obs.closeReason == WebSocketCloseConstants::kDecompressionErrorMessage);
        REQUIRE(obs.pongCount == 0);
        REQUIRE(obs.unexpectedFrameCount == 0);
    }
}
