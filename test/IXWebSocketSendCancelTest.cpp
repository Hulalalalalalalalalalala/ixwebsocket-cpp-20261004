/*
 *  IXWebSocketSendCancelTest.cpp
 *
 *  Cancelling a fragmented send through the progress callback: the send
 *  fails, the remaining fragments are never sent, and the connection is
 *  closed with code 1000 and reason "Send cancelled".
 */

#include "IXTest.h"
#include <catch_amalgamated.hpp>
#include <ixwebsocket/IXWebSocket.h>
#include <ixwebsocket/IXWebSocketSendInfo.h>
#include <ixwebsocket/IXWebSocketServer.h>
#include <atomic>
#include <chrono>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#ifndef _WIN32
#include <cstring>
#include <ixwebsocket/IXWebSocketHandshakeKeyGen.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

using namespace ix;

namespace
{
    const uint16_t kNormalClosureCode = 1000;
    const std::string kSendCancelledReason("Send cancelled");

    struct CloseRecorder
    {
        // Written before count is incremented; readers must observe count
        // first (it is atomic) and only then read the other fields.
        std::atomic<int> count {0};
        uint16_t code {0};
        std::string reason;
        bool remote {false};

        void record(uint16_t c, const std::string& r, bool rem)
        {
            code = c;
            reason = r;
            remote = rem;
            count++;
        }
    };

    struct MessageRecorder
    {
        std::atomic<int> count {0};
        std::atomic<int> errorCount {0};
        std::string last;

        void record(const std::string& msg)
        {
            last = msg;
            count++;
        }
    };

    bool waitFor(const std::function<bool()>& predicate, int timeoutMs = 10000)
    {
        int waited = 0;
        while (!predicate() && waited < timeoutMs)
        {
            msleep(5);
            waited += 5;
        }
        return predicate();
    }

    // Incompressible payload (xorshift PRNG output)
    std::string makeRandomPayload(size_t size)
    {
        std::string data(size, ' ');
        uint64_t x = 0x9e3779b97f4a7c15ULL;
        for (size_t i = 0; i < size; ++i)
        {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            data[i] = static_cast<char>(x & 0xff);
        }
        return data;
    }

    struct RecordingServer
    {
        ix::WebSocketServer server;
        MessageRecorder messages;
        CloseRecorder closeEvents;

        RecordingServer(int port)
            : server(port, "127.0.0.1")
        {
            server.setOnClientMessageCallback(
                [this](std::shared_ptr<ConnectionState>,
                       WebSocket&,
                       const WebSocketMessagePtr& msg)
                {
                    if (msg->type == WebSocketMessageType::Message)
                    {
                        messages.record(msg->str);
                    }
                    else if (msg->type == WebSocketMessageType::Error)
                    {
                        messages.errorCount++;
                    }
                    else if (msg->type == WebSocketMessageType::Close)
                    {
                        closeEvents.record(
                            msg->closeInfo.code, msg->closeInfo.reason, msg->closeInfo.remote);
                    }
                });
        }

        bool start()
        {
            return server.listenAndStart();
        }

        void stop()
        {
            server.stop();
        }
    };

    struct TestClient
    {
        ix::WebSocket webSocket;
        CloseRecorder closeEvents;
        MessageRecorder messages;
        std::atomic<bool> open {false};

        TestClient(const std::string& url)
        {
            webSocket.setUrl(url);
            webSocket.disableAutomaticReconnection();
            webSocket.setOnMessageCallback(
                [this](const WebSocketMessagePtr& msg)
                {
                    if (msg->type == WebSocketMessageType::Open)
                    {
                        open = true;
                    }
                    else if (msg->type == WebSocketMessageType::Message)
                    {
                        messages.record(msg->str);
                    }
                    else if (msg->type == WebSocketMessageType::Error)
                    {
                        messages.errorCount++;
                    }
                    else if (msg->type == WebSocketMessageType::Close)
                    {
                        closeEvents.record(
                            msg->closeInfo.code, msg->closeInfo.reason, msg->closeInfo.remote);
                    }
                });
        }
    };
} // namespace

TEST_CASE("SendCancel")
{
    SECTION("client cancel on first fragment fails the send and closes the connection")
    {
        int port = getFreePort();
        RecordingServer server(port);
        REQUIRE(server.start());

        TestClient client("ws://127.0.0.1:" + std::to_string(port) + "/");
        client.webSocket.start();
        REQUIRE(waitFor([&] { return client.open.load(); }));

        std::string big = makeRandomPayload(200 * 1024);
        std::atomic<int> progressCalls {0};

        WebSocketSendInfo info = client.webSocket.sendBinary(
            big,
            [&](int, int)
            {
                progressCalls++;
                return false; // cancel right away
            });

        // The send fails, without being reported as a compression error,
        // and the progress callback is not invoked anymore.
        REQUIRE_FALSE(info.success);
        REQUIRE_FALSE(info.compressionError);
        REQUIRE(progressCalls == 1);

        // The connection is already leaving the Open state when send returns
        REQUIRE(client.webSocket.getReadyState() != ix::ReadyState::Open);

        // Sending on the cancelled connection fails
        REQUIRE_FALSE(client.webSocket.sendText("after cancel").success);

        // The close is reported exactly once, initiated locally, with the
        // normal closure code and the cancellation reason.
        REQUIRE(waitFor([&] { return client.closeEvents.count == 1; }));
        REQUIRE(client.closeEvents.code == kNormalClosureCode);
        REQUIRE(client.closeEvents.reason == kSendCancelledReason);
        REQUIRE(client.closeEvents.remote == false);
        REQUIRE(client.webSocket.bufferedAmount() == 0);

        // The server cannot reassemble a full message out of the prefix it
        // received, and sees the close with the same code and reason.
        REQUIRE(waitFor([&] { return server.closeEvents.count == 1; }));
        REQUIRE(server.messages.count == 0);
        REQUIRE(server.messages.errorCount == 0);
        REQUIRE(server.closeEvents.code == kNormalClosureCode);
        REQUIRE(server.closeEvents.reason == kSendCancelledReason);
        REQUIRE(server.closeEvents.remote == true);

        client.webSocket.stop();
        server.stop();
    }

    SECTION("cancel in the middle fragment fails the send and closes the connection")
    {
        int port = getFreePort();
        RecordingServer server(port);
        REQUIRE(server.start());

        TestClient client("ws://127.0.0.1:" + std::to_string(port) + "/");
        client.webSocket.start();
        REQUIRE(waitFor([&] { return client.open.load(); }));

        std::string big = makeRandomPayload(200 * 1024);
        std::atomic<int> progressCalls {0};

        // 200K / 32K == 6 fragments; cancel after the third one
        WebSocketSendInfo info = client.webSocket.sendBinary(
            big, [&](int current, int) {
                progressCalls++;
                return current < 2;
            });

        REQUIRE_FALSE(info.success);
        REQUIRE_FALSE(info.compressionError);
        REQUIRE(progressCalls == 3);
        REQUIRE(client.webSocket.getReadyState() != ix::ReadyState::Open);

        REQUIRE(waitFor([&] { return client.closeEvents.count == 1; }));
        REQUIRE(client.closeEvents.code == kNormalClosureCode);
        REQUIRE(client.closeEvents.reason == kSendCancelledReason);
        REQUIRE(client.closeEvents.remote == false);
        REQUIRE(client.webSocket.bufferedAmount() == 0);

        REQUIRE(waitFor([&] { return server.closeEvents.count == 1; }));
        REQUIRE(server.messages.count == 0);
        REQUIRE(server.closeEvents.code == kNormalClosureCode);
        REQUIRE(server.closeEvents.reason == kSendCancelledReason);

        client.webSocket.stop();
        server.stop();
    }

    SECTION("returning false on the last fragment is a successful send")
    {
        int port = getFreePort();
        RecordingServer server(port);
        REQUIRE(server.start());

        TestClient client("ws://127.0.0.1:" + std::to_string(port) + "/");
        client.webSocket.start();
        REQUIRE(waitFor([&] { return client.open.load(); }));

        std::string big = makeRandomPayload(70000);
        std::atomic<int> progressCalls {0};

        WebSocketSendInfo info = client.webSocket.sendBinary(
            big,
            [&](int current, int total)
            {
                progressCalls++;
                // Only return false once the last fragment was committed
                return current + 1 < total;
            });

        // The message was fully committed: success, and the connection
        // stays usable.
        REQUIRE(info.success);
        REQUIRE(progressCalls == 2);
        REQUIRE(client.webSocket.getReadyState() == ix::ReadyState::Open);

        REQUIRE(waitFor([&] { return server.messages.count == 1; }));
        REQUIRE(server.messages.last == big);

        // The connection is still usable afterwards
        REQUIRE(client.webSocket.sendText("still open").success);
        REQUIRE(waitFor([&] { return server.messages.count == 2; }));
        REQUIRE(server.messages.last == "still open");

        client.webSocket.stop();
        server.stop();
    }

    SECTION("returning false on a single frame message is a successful send")
    {
        int port = getFreePort();
        RecordingServer server(port);
        REQUIRE(server.start());

        TestClient client("ws://127.0.0.1:" + std::to_string(port) + "/");
        client.webSocket.start();
        REQUIRE(waitFor([&] { return client.open.load(); }));

        std::atomic<int> progressCalls {0};
        WebSocketSendInfo info = client.webSocket.sendText(
            "small message",
            [&](int, int)
            {
                progressCalls++;
                return false;
            });

        REQUIRE(info.success);
        REQUIRE(progressCalls == 1);
        REQUIRE(client.webSocket.getReadyState() == ix::ReadyState::Open);

        REQUIRE(waitFor([&] { return server.messages.count == 1; }));
        REQUIRE(server.messages.last == "small message");

        client.webSocket.stop();
        server.stop();
    }

    SECTION("returning false on an empty message is a successful send")
    {
        int port = getFreePort();
        RecordingServer server(port);
        REQUIRE(server.start());

        TestClient client("ws://127.0.0.1:" + std::to_string(port) + "/");
        client.webSocket.start();
        REQUIRE(waitFor([&] { return client.open.load(); }));

        std::atomic<int> progressCalls {0};
        WebSocketSendInfo info = client.webSocket.sendText(
            "",
            [&](int, int)
            {
                progressCalls++;
                return false;
            });

        REQUIRE(info.success);
        REQUIRE(progressCalls == 1);
        REQUIRE(client.webSocket.getReadyState() == ix::ReadyState::Open);

        REQUIRE(waitFor([&] { return server.messages.count == 1; }));
        REQUIRE(server.messages.last == "");

        client.webSocket.stop();
        server.stop();
    }

    SECTION("a send queued from another thread fails after the cancellation")
    {
        int port = getFreePort();
        RecordingServer server(port);
        REQUIRE(server.start());

        TestClient client("ws://127.0.0.1:" + std::to_string(port) + "/");
        client.webSocket.start();
        REQUIRE(waitFor([&] { return client.open.load(); }));

        std::string big = makeRandomPayload(200 * 1024);
        std::atomic<bool> inCallback {false};
        std::atomic<bool> releaseCallback {false};

        std::thread sender(
            [&]()
            {
                client.webSocket.sendBinary(
                    big,
                    [&](int, int)
                    {
                        inCallback = true;
                        while (!releaseCallback.load())
                        {
                            std::this_thread::sleep_for(std::chrono::milliseconds(1));
                        }
                        return false;
                    });
            });

        REQUIRE(waitFor([&] { return inCallback.load(); }));

        // This send is issued while the cancelled send still holds the write
        // mutex; it must not write anything to the closing connection.
        WebSocketSendInfo queuedInfo;
        std::atomic<bool> queuedDone {false};
        std::thread queued(
            [&]()
            {
                queuedInfo = client.webSocket.sendText("queued message");
                queuedDone = true;
            });

        // Give the queued send time to block on the write mutex
        msleep(200);
        releaseCallback = true;

        sender.join();
        queued.join();

        REQUIRE(queuedDone.load());
        REQUIRE_FALSE(queuedInfo.success);

        REQUIRE(waitFor([&] { return client.closeEvents.count == 1; }));
        REQUIRE(server.messages.count == 0);

        client.webSocket.stop();
        server.stop();
    }

    SECTION("server cancel on first fragment fails the send and closes the connection")
    {
        int port = getFreePort();
        RecordingServer server(port);
        REQUIRE(server.start());

        TestClient client("ws://127.0.0.1:" + std::to_string(port) + "/");
        client.webSocket.start();
        REQUIRE(waitFor([&] { return client.open.load(); }));
        REQUIRE(waitFor([&] { return server.server.getClients().size() == 1; }));

        auto clients = server.server.getClients();
        auto serverWebSocket = *clients.begin();

        std::string big = makeRandomPayload(200 * 1024);
        std::atomic<int> progressCalls {0};

        WebSocketSendInfo info = serverWebSocket->sendBinary(
            big,
            [&](int, int)
            {
                progressCalls++;
                return false;
            });

        REQUIRE_FALSE(info.success);
        REQUIRE_FALSE(info.compressionError);
        REQUIRE(progressCalls == 1);
        REQUIRE(serverWebSocket->getReadyState() != ix::ReadyState::Open);

        // The client never receives a full message and sees the close
        // initiated by the server.
        REQUIRE(waitFor([&] { return client.closeEvents.count == 1; }));
        REQUIRE(client.messages.count == 0);
        REQUIRE(client.messages.errorCount == 0);
        REQUIRE(client.closeEvents.code == kNormalClosureCode);
        REQUIRE(client.closeEvents.reason == kSendCancelledReason);
        REQUIRE(client.closeEvents.remote == true);

        REQUIRE(waitFor([&] { return server.closeEvents.count == 1; }));
        REQUIRE(server.closeEvents.code == kNormalClosureCode);
        REQUIRE(server.closeEvents.reason == kSendCancelledReason);
        REQUIRE(server.closeEvents.remote == false);
        REQUIRE(serverWebSocket->bufferedAmount() == 0);

        client.webSocket.stop();
        server.stop();
    }

    SECTION("reconnecting with the same client object delivers only new content")
    {
        int port = getFreePort();
        RecordingServer server(port);
        REQUIRE(server.start());

        TestClient client("ws://127.0.0.1:" + std::to_string(port) + "/");
        client.webSocket.enablePerMessageDeflate();
        client.webSocket.start();
        REQUIRE(waitFor([&] { return client.open.load(); }));

        // Cancel a fragmented, compressed send
        std::string big = makeRandomPayload(200 * 1024);
        WebSocketSendInfo info = client.webSocket.sendBinary(
            big, [&](int, int) { return false; });
        REQUIRE_FALSE(info.success);

        REQUIRE(waitFor([&] { return client.closeEvents.count == 1; }));
        client.webSocket.stop();

        // Reconnect with the same client object
        client.open = false;
        client.webSocket.start();
        REQUIRE(waitFor([&] { return client.open.load(); }));

        // A compressible message sent on the new connection must be
        // decompressed and delivered in full; no stale fragment, close
        // frame or compression state may leak into the new connection.
        std::string text(100000, 'x');
        REQUIRE(client.webSocket.sendText(text).success);
        REQUIRE(waitFor([&] { return server.messages.count == 1; }));
        REQUIRE(server.messages.last == text);
        REQUIRE(server.messages.errorCount == 0);

        client.webSocket.stop();
        server.stop();
    }

    SECTION("per-message-deflate: cancelled compressed send is not delivered nor a compression error")
    {
        int port = getFreePort();
        RecordingServer server(port);
        REQUIRE(server.start());

        TestClient client("ws://127.0.0.1:" + std::to_string(port) + "/");
        client.webSocket.enablePerMessageDeflate();
        client.webSocket.start();
        REQUIRE(waitFor([&] { return client.open.load(); }));

        // Incompressible payload: stays fragmented after compression
        std::string big = makeRandomPayload(200 * 1024);
        WebSocketSendInfo info = client.webSocket.sendBinary(
            big, [&](int, int) { return false; });

        REQUIRE_FALSE(info.success);
        REQUIRE_FALSE(info.compressionError);

        REQUIRE(waitFor([&] { return client.closeEvents.count == 1; }));
        REQUIRE(client.closeEvents.code == kNormalClosureCode);
        REQUIRE(client.closeEvents.reason == kSendCancelledReason);

        // The compressed prefix must not be delivered as a full message,
        // and must not raise a decompression error either.
        REQUIRE(waitFor([&] { return server.closeEvents.count == 1; }));
        REQUIRE(server.messages.count == 0);
        REQUIRE(server.messages.errorCount == 0);
        REQUIRE(server.closeEvents.code == kNormalClosureCode);
        REQUIRE(server.closeEvents.reason == kSendCancelledReason);

        client.webSocket.stop();
        server.stop();
    }

    SECTION("per-message-deflate: large message compressed to a single frame is a successful send")
    {
        int port = getFreePort();
        RecordingServer server(port);
        REQUIRE(server.start());

        TestClient client("ws://127.0.0.1:" + std::to_string(port) + "/");
        client.webSocket.enablePerMessageDeflate();
        client.webSocket.start();
        REQUIRE(waitFor([&] { return client.open.load(); }));

        // Large but highly compressible: fits in a single frame once
        // compressed, so the send is complete even if the callback of the
        // only fragment returns false.
        std::string big(1000 * 1000, 'a');
        std::atomic<int> progressCalls {0};
        WebSocketSendInfo info = client.webSocket.sendText(
            big,
            [&](int, int)
            {
                progressCalls++;
                return false;
            });

        REQUIRE(info.success);
        REQUIRE(progressCalls == 1);
        REQUIRE(client.webSocket.getReadyState() == ix::ReadyState::Open);

        REQUIRE(waitFor([&] { return server.messages.count == 1; }));
        REQUIRE(server.messages.last == big);

        client.webSocket.stop();
        server.stop();
    }

#if defined(IXWEBSOCKET_USE_OPEN_SSL) || defined(IXWEBSOCKET_USE_MBED_TLS)
    SECTION("cancel over TLS fails the send and closes the connection")
    {
        int port = getFreePort();
        RecordingServer server(port);
        // The bundled test certificates have expired; what matters here is
        // exercising the TLS transport, not certificate validation.
        SocketTLSOptions serverTlsOptions = makeServerTLSOptions(true);
        serverTlsOptions.caFile = "NONE";
        server.server.setTLSOptions(serverTlsOptions);
        REQUIRE(server.start());

        TestClient client("wss://127.0.0.1:" + std::to_string(port) + "/");
        SocketTLSOptions tlsOptions;
        tlsOptions.caFile = "NONE";
        tlsOptions.disable_hostname_validation = true;
        client.webSocket.setTLSOptions(tlsOptions);
        client.webSocket.start();
        REQUIRE(waitFor([&] { return client.open.load(); }));

        std::string big = makeRandomPayload(200 * 1024);
        WebSocketSendInfo info = client.webSocket.sendBinary(
            big, [&](int, int) { return false; });

        REQUIRE_FALSE(info.success);
        REQUIRE_FALSE(info.compressionError);
        REQUIRE(client.webSocket.getReadyState() != ix::ReadyState::Open);

        REQUIRE(waitFor([&] { return client.closeEvents.count == 1; }));
        REQUIRE(client.closeEvents.code == kNormalClosureCode);
        REQUIRE(client.closeEvents.reason == kSendCancelledReason);
        REQUIRE(client.closeEvents.remote == false);

        REQUIRE(waitFor([&] { return server.closeEvents.count == 1; }));
        REQUIRE(server.messages.count == 0);
        REQUIRE(server.closeEvents.code == kNormalClosureCode);
        REQUIRE(server.closeEvents.reason == kSendCancelledReason);

        client.webSocket.stop();
        server.stop();
    }
#endif

#ifndef _WIN32
    SECTION("the peer receives the remaining fragment bytes, then the close frame")
    {
        // A raw TCP peer: it accepts the WebSocket handshake, then reads
        // the wire bytes itself so that we can inspect the exact frames.
        int port = getFreePort();

        int listenFd = socket(AF_INET, SOCK_STREAM, 0);
        REQUIRE(listenFd >= 0);
        int one = 1;
        setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

        sockaddr_in addr;
        std::memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = htons(static_cast<uint16_t>(port));
        REQUIRE(bind(listenFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
        REQUIRE(listen(listenFd, 1) == 0);

        std::vector<uint8_t> received;
        std::atomic<bool> peerDone {false};
        std::thread peerThread(
            [&]()
            {
                int conn = accept(listenFd, nullptr, nullptr);
                if (conn < 0)
                {
                    peerDone = true;
                    return;
                }

                // Read the HTTP upgrade request
                std::string request;
                char buf[4096];
                while (request.find("\r\n\r\n") == std::string::npos)
                {
                    ssize_t n = recv(conn, buf, sizeof(buf), 0);
                    if (n <= 0)
                    {
                        close(conn);
                        peerDone = true;
                        return;
                    }
                    request.append(buf, buf + n);
                }

                // Extract the Sec-WebSocket-Key header
                std::string keyHeader("Sec-WebSocket-Key: ");
                auto keyPos = request.find(keyHeader);
                REQUIRE(keyPos != std::string::npos);
                auto keyEnd = request.find("\r\n", keyPos);
                std::string key =
                    request.substr(keyPos + keyHeader.size(), keyEnd - keyPos - keyHeader.size());

                char acceptKey[29] = {};
                WebSocketHandshakeKeyGen::generate(key, acceptKey);

                std::string response;
                response += "HTTP/1.1 101 Switching Protocols\r\n";
                response += "Upgrade: websocket\r\n";
                response += "Connection: Upgrade\r\n";
                response += "Sec-WebSocket-Accept: " + std::string(acceptKey, 28) + "\r\n";
                response += "\r\n";
                REQUIRE(send(conn, response.data(), response.size(), 0) ==
                        (ssize_t) response.size());

                // Do not read for a while: the client cancels meanwhile and
                // queues the close frame behind the fragment bytes.
                msleep(100);

                // Read everything until the client closes the connection
                // (the peer never answers the close frame, so the client
                // closes after its closing delay).
                while (true)
                {
                    ssize_t n = recv(conn, buf, sizeof(buf), 0);
                    if (n <= 0) break;
                    received.insert(received.end(), buf, buf + n);
                }

                close(conn);
                peerDone = true;
            });

        TestClient client("ws://127.0.0.1:" + std::to_string(port) + "/");
        client.webSocket.start();
        REQUIRE(waitFor([&] { return client.open.load(); }));

        std::string big = makeRandomPayload(200 * 1024);
        WebSocketSendInfo info = client.webSocket.sendBinary(
            big, [&](int, int) { return false; });
        REQUIRE_FALSE(info.success);

        // The peer never replies to the close frame: the client must still
        // finish closing after its closing delay, and report the close once.
        REQUIRE(waitFor([&] { return client.closeEvents.count == 1; }));
        REQUIRE(client.closeEvents.code == kNormalClosureCode);
        REQUIRE(client.closeEvents.reason == kSendCancelledReason);
        REQUIRE(client.closeEvents.remote == false);
        REQUIRE(client.webSocket.bufferedAmount() == 0);

        REQUIRE(waitFor([&] { return peerDone.load(); }));
        peerThread.join();
        close(listenFd);

        // Parse the frames the peer received
        struct Frame
        {
            bool fin;
            uint8_t opcode;
            std::string payload;
        };
        std::vector<Frame> frames;

        size_t pos = 0;
        while (pos + 2 <= received.size())
        {
            Frame frame;
            frame.fin = (received[pos] & 0x80) != 0;
            frame.opcode = received[pos] & 0x0f;
            bool masked = (received[pos + 1] & 0x80) != 0;
            uint64_t len = received[pos + 1] & 0x7f;
            size_t headerSize = 2;
            if (len == 126)
            {
                REQUIRE(pos + 4 <= received.size());
                len = ((uint64_t) received[pos + 2] << 8) | received[pos + 3];
                headerSize += 2;
            }
            else if (len == 127)
            {
                REQUIRE(pos + 10 <= received.size());
                len = 0;
                for (int i = 0; i < 8; ++i)
                {
                    len |= (uint64_t) received[pos + 2 + i] << (56 - 8 * i);
                }
                headerSize += 8;
            }

            uint8_t mask[4] = {0, 0, 0, 0};
            if (masked)
            {
                REQUIRE(pos + headerSize + 4 <= received.size());
                std::memcpy(mask, &received[pos + headerSize], 4);
                headerSize += 4;
            }

            REQUIRE(pos + headerSize + len <= received.size());
            frame.payload.assign(reinterpret_cast<const char*>(&received[pos + headerSize]),
                                 (size_t) len);
            for (size_t i = 0; i < (size_t) len; ++i)
            {
                frame.payload[i] ^= mask[i & 0x3];
            }

            frames.push_back(std::move(frame));
            pos += headerSize + len;
        }

        // No garbage and no half frame at the end of the stream
        REQUIRE(pos == received.size());

        // Exactly one non-final data fragment, then the close frame: the
        // sent prefix was not completed into a truncated full message, and
        // the close frame was not mixed into the fragment payload.
        REQUIRE(frames.size() == 2);

        REQUIRE_FALSE(frames[0].fin);
        REQUIRE(frames[0].opcode == 0x2); // binary
        REQUIRE(frames[0].payload.size() == 32768);
        REQUIRE(frames[0].payload == big.substr(0, 32768));

        REQUIRE(frames[1].fin);
        REQUIRE(frames[1].opcode == 0x8); // close
        REQUIRE(frames[1].payload.size() == 2 + kSendCancelledReason.size());
        REQUIRE(frames[1].payload[0] == (char) (kNormalClosureCode >> 8));
        REQUIRE(frames[1].payload[1] == (char) (kNormalClosureCode & 0xff));
        REQUIRE(frames[1].payload.substr(2) == kSendCancelledReason);

        client.webSocket.stop();
    }
#endif
}
