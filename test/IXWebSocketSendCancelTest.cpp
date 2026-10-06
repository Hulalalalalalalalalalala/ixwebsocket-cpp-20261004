/*
 *  IXWebSocketSendCancelTest.cpp
 *
 *  Test cancelling a large (fragmented) message send from the
 *  progress callback.
 */

#include "IXTest.h"
#include <atomic>
#include <catch_amalgamated.hpp>
#include <ixwebsocket/IXSocket.h>
#include <ixwebsocket/IXSocketFactory.h>
#include <ixwebsocket/IXWebSocket.h>
#include <ixwebsocket/IXWebSocketServer.h>
#include <mutex>
#include <thread>
#include <vector>

using namespace ix;

namespace
{
    struct CloseEvent
    {
        uint16_t code;
        std::string reason;
        bool remote;
    };

    // Records Close events and received (complete) messages.
    struct MessageRecorder
    {
        mutable std::mutex mutex;
        std::vector<CloseEvent> closeEvents;
        std::vector<std::string> messages;
        std::atomic<int> openCount {0};

        void recordClose(uint16_t code, const std::string& reason, bool remote)
        {
            std::lock_guard<std::mutex> lock(mutex);
            closeEvents.push_back({code, reason, remote});
        }

        void recordMessage(const std::string& str)
        {
            std::lock_guard<std::mutex> lock(mutex);
            messages.push_back(str);
        }

        size_t closeEventCount() const
        {
            std::lock_guard<std::mutex> lock(mutex);
            return closeEvents.size();
        }

        CloseEvent lastCloseEvent() const
        {
            std::lock_guard<std::mutex> lock(mutex);
            return closeEvents.back();
        }

        size_t messageCount() const
        {
            std::lock_guard<std::mutex> lock(mutex);
            return messages.size();
        }

        std::string lastMessage() const
        {
            std::lock_guard<std::mutex> lock(mutex);
            return messages.back();
        }
    };

    bool startServer(ix::WebSocketServer& server, MessageRecorder& recorder)
    {
        server.setOnClientMessageCallback(
            [&recorder](std::shared_ptr<ConnectionState> /*connectionState*/,
                        WebSocket& /*webSocket*/,
                        const ix::WebSocketMessagePtr& msg) {
                if (msg->type == ix::WebSocketMessageType::Open)
                {
                    recorder.openCount++;
                }
                else if (msg->type == ix::WebSocketMessageType::Close)
                {
                    recorder.recordClose(
                        msg->closeInfo.code, msg->closeInfo.reason, msg->closeInfo.remote);
                }
                else if (msg->type == ix::WebSocketMessageType::Message)
                {
                    recorder.recordMessage(msg->str);
                }
            });

        auto res = server.listen();
        if (!res.first)
        {
            log(res.second);
            return false;
        }

        server.start();
        return true;
    }

    void setupClient(ix::WebSocket& webSocket, int port, MessageRecorder& recorder)
    {
        std::string url;
        {
            std::stringstream ss;
            ss << "ws://localhost:" << port << "/";
            url = ss.str();
        }

        webSocket.setUrl(url);
        webSocket.disableAutomaticReconnection();

        webSocket.setOnMessageCallback([&recorder](const ix::WebSocketMessagePtr& msg) {
            if (msg->type == ix::WebSocketMessageType::Open)
            {
                recorder.openCount++;
            }
            else if (msg->type == ix::WebSocketMessageType::Close)
            {
                recorder.recordClose(
                    msg->closeInfo.code, msg->closeInfo.reason, msg->closeInfo.remote);
            }
            else if (msg->type == ix::WebSocketMessageType::Message)
            {
                recorder.recordMessage(msg->str);
            }
        });
    }

    bool waitForOpen(ix::WebSocket& webSocket)
    {
        for (int i = 0; i < 200; ++i)
        {
            if (webSocket.getReadyState() == ix::ReadyState::Open) return true;
            ix::msleep(10);
        }
        return false;
    }

    bool waitForClose(const MessageRecorder& recorder)
    {
        for (int i = 0; i < 400; ++i)
        {
            if (recorder.closeEventCount() > 0) return true;
            ix::msleep(10);
        }
        return false;
    }

    // A string that does not compress well, so that a per-message-deflate
    // enabled connection still fragments it.
    std::string makeRandomString(size_t size)
    {
        std::string str;
        str.reserve(size);
        unsigned int seed = 42;
        for (size_t i = 0; i < size; ++i)
        {
            seed = seed * 1103515245 + 12345;
            str += static_cast<char>((seed >> 16) & 0xff);
        }
        return str;
    }
} // namespace

TEST_CASE("Websocket_send_cancel_client", "[send_cancel]")
{
    SECTION("Cancelling from the progress callback closes the connection with 1000")
    {
        int port = getFreePort();
        ix::WebSocketServer server(port);

        MessageRecorder serverRecorder;
        REQUIRE(startServer(server, serverRecorder));

        MessageRecorder clientRecorder;
        ix::WebSocket client;
        setupClient(client, port, clientRecorder);
        client.start();

        REQUIRE(waitForOpen(client));

        // 10 fragments worth of data (fragments are 32K)
        std::string bigMessage(10 * 32768, 'a');

        int callbackCount = 0;
        WebSocketSendInfo info = client.sendText(bigMessage, [&callbackCount](int, int) {
            callbackCount++;
            return false; // cancel right away, on the first fragment
        });

        // The send was cancelled: it failed, without a compression error,
        // and the connection is not usable anymore.
        REQUIRE(info.success == false);
        REQUIRE(info.compressionError == false);
        REQUIRE(client.getReadyState() != ix::ReadyState::Open);

        // The progress callback must not be called again for this send.
        REQUIRE(callbackCount == 1);

        // Sending on a cancelled connection fails.
        REQUIRE(client.sendText("hello").success == false);

        // We get a single close event, initiated by us, with the normal
        // closure code and the "Send cancelled" reason.
        REQUIRE(waitForClose(clientRecorder));
        REQUIRE(clientRecorder.closeEventCount() == 1);

        CloseEvent closeEvent = clientRecorder.lastCloseEvent();
        REQUIRE(closeEvent.code == 1000);
        REQUIRE(closeEvent.reason == "Send cancelled");
        REQUIRE(closeEvent.remote == false);

        // Nothing is left to write once the connection is closed.
        for (int i = 0; i < 100 && client.bufferedAmount() != 0; ++i)
        {
            ix::msleep(10);
        }
        REQUIRE(client.bufferedAmount() == 0);

        // The server saw the close, initiated by the client, with the same
        // code and reason, and never received a complete message.
        REQUIRE(waitForClose(serverRecorder));
        CloseEvent serverCloseEvent = serverRecorder.lastCloseEvent();
        REQUIRE(serverCloseEvent.code == 1000);
        REQUIRE(serverCloseEvent.reason == "Send cancelled");
        REQUIRE(serverCloseEvent.remote == true);
        REQUIRE(serverRecorder.messageCount() == 0);

        client.stop();
        server.stop();
    }

    SECTION("Cancelling on the last fragment is a no-op: the send succeeds")
    {
        int port = getFreePort();
        ix::WebSocketServer server(port);

        MessageRecorder serverRecorder;
        REQUIRE(startServer(server, serverRecorder));

        MessageRecorder clientRecorder;
        ix::WebSocket client;
        setupClient(client, port, clientRecorder);
        client.start();

        REQUIRE(waitForOpen(client));

        std::string bigMessage(10 * 32768, 'b');

        WebSocketSendInfo info =
            client.sendText(bigMessage, [](int current, int total) -> bool {
                // Only return false once the last fragment was committed
                return current + 1 < total;
            });

        REQUIRE(info.success == true);
        REQUIRE(client.getReadyState() == ix::ReadyState::Open);

        // The full message is delivered to the server.
        for (int i = 0; i < 200 && serverRecorder.messageCount() == 0; ++i)
        {
            ix::msleep(10);
        }
        REQUIRE(serverRecorder.messageCount() == 1);
        REQUIRE(serverRecorder.lastMessage() == bigMessage);

        // The connection is still usable.
        REQUIRE(client.sendText("hello").success == true);
        for (int i = 0; i < 200 && serverRecorder.messageCount() < 2; ++i)
        {
            ix::msleep(10);
        }
        REQUIRE(serverRecorder.messageCount() == 2);
        REQUIRE(serverRecorder.lastMessage() == "hello");

        client.stop();
        server.stop();
    }

    SECTION("A false return for a single frame message does not cancel")
    {
        int port = getFreePort();
        ix::WebSocketServer server(port);

        MessageRecorder serverRecorder;
        REQUIRE(startServer(server, serverRecorder));

        MessageRecorder clientRecorder;
        ix::WebSocket client;
        setupClient(client, port, clientRecorder);
        client.start();

        REQUIRE(waitForOpen(client));

        WebSocketSendInfo info = client.sendText("small", [](int, int) { return false; });
        REQUIRE(info.success == true);
        REQUIRE(client.getReadyState() == ix::ReadyState::Open);

        // Empty message: the unique callback cannot cancel either.
        WebSocketSendInfo emptyInfo = client.sendText("", [](int, int) { return false; });
        REQUIRE(emptyInfo.success == true);
        REQUIRE(client.getReadyState() == ix::ReadyState::Open);

        for (int i = 0; i < 200 && serverRecorder.messageCount() < 2; ++i)
        {
            ix::msleep(10);
        }
        REQUIRE(serverRecorder.messageCount() == 2);
        REQUIRE(serverRecorder.lastMessage() == "");

        client.stop();
        server.stop();
    }

    SECTION("A message waiting on a cancelled send cannot be written")
    {
        int port = getFreePort();
        ix::WebSocketServer server(port);

        MessageRecorder serverRecorder;
        REQUIRE(startServer(server, serverRecorder));

        MessageRecorder clientRecorder;
        ix::WebSocket client;
        setupClient(client, port, clientRecorder);
        client.start();

        REQUIRE(waitForOpen(client));

        std::string bigMessage(10 * 32768, 'c');

        std::atomic<bool> callbackEntered {false};
        std::atomic<bool> secondSendDone {false};
        std::atomic<bool> secondSendSuccess {true};

        std::thread secondSender([&]() {
            // Wait until the first send is holding the write lock, then
            // queue up behind it.
            while (!callbackEntered)
            {
            }
            ix::msleep(50);
            WebSocketSendInfo info = client.sendText("queued message");
            secondSendSuccess = info.success;
            secondSendDone = true;
        });

        WebSocketSendInfo info = client.sendText(bigMessage, [&](int, int) {
            callbackEntered = true;
            // Give the second thread a chance to block on the write mutex.
            for (int i = 0; i < 200 && !secondSendDone; ++i)
            {
                ix::msleep(10);
            }
            return false;
        });

        secondSender.join();

        REQUIRE(info.success == false);
        REQUIRE(secondSendDone == true);
        REQUIRE(secondSendSuccess == false);

        REQUIRE(waitForClose(clientRecorder));
        REQUIRE(serverRecorder.messageCount() == 0);

        client.stop();
        server.stop();
    }

    SECTION("Cancelling also works over TLS")
    {
#if defined(IXWEBSOCKET_USE_OPEN_SSL) || defined(IXWEBSOCKET_USE_MBED_TLS)
        int port = getFreePort();
        ix::WebSocketServer server(port);
        server.setTLSOptions(makeServerTLSOptions(true));

        MessageRecorder serverRecorder;
        REQUIRE(startServer(server, serverRecorder));

        MessageRecorder clientRecorder;
        ix::WebSocket client;
        {
            std::stringstream ss;
            ss << "wss://localhost:" << port << "/";
            client.setUrl(ss.str());
        }
        client.setTLSOptions(makeClientTLSOptions());
        client.disableAutomaticReconnection();
        client.setOnMessageCallback([&clientRecorder](const ix::WebSocketMessagePtr& msg) {
            if (msg->type == ix::WebSocketMessageType::Close)
            {
                clientRecorder.recordClose(
                    msg->closeInfo.code, msg->closeInfo.reason, msg->closeInfo.remote);
            }
            else if (msg->type == ix::WebSocketMessageType::Message)
            {
                clientRecorder.recordMessage(msg->str);
            }
        });
        client.start();

        REQUIRE(waitForOpen(client));

        std::string bigMessage(10 * 32768, 'f');

        WebSocketSendInfo info = client.sendText(bigMessage, [](int, int) { return false; });

        REQUIRE(info.success == false);
        REQUIRE(info.compressionError == false);
        REQUIRE(client.getReadyState() != ix::ReadyState::Open);

        REQUIRE(waitForClose(clientRecorder));
        REQUIRE(clientRecorder.closeEventCount() == 1);

        CloseEvent closeEvent = clientRecorder.lastCloseEvent();
        REQUIRE(closeEvent.code == 1000);
        REQUIRE(closeEvent.reason == "Send cancelled");
        REQUIRE(closeEvent.remote == false);

        REQUIRE(waitForClose(serverRecorder));
        REQUIRE(serverRecorder.messageCount() == 0);

        client.stop();
        server.stop();
#endif
    }
}


TEST_CASE("Websocket_send_cancel_per_message_deflate", "[send_cancel]")
{
    SECTION("Cancelling a compressed message is not a compression error")
    {
        int port = getFreePort();
        ix::WebSocketServer server(port);

        MessageRecorder serverRecorder;
        REQUIRE(startServer(server, serverRecorder));

        MessageRecorder clientRecorder;
        ix::WebSocket client;
        setupClient(client, port, clientRecorder);
        client.enablePerMessageDeflate();
        client.start();

        REQUIRE(waitForOpen(client));

        // Random data does not compress: the compressed payload is still
        // fragmented.
        std::string bigMessage = makeRandomString(10 * 32768);

        WebSocketSendInfo info = client.sendBinary(bigMessage, [](int, int) { return false; });

        REQUIRE(info.success == false);
        REQUIRE(info.compressionError == false);
        REQUIRE(client.getReadyState() != ix::ReadyState::Open);

        REQUIRE(waitForClose(clientRecorder));
        REQUIRE(clientRecorder.closeEventCount() == 1);

        CloseEvent closeEvent = clientRecorder.lastCloseEvent();
        REQUIRE(closeEvent.code == 1000);
        REQUIRE(closeEvent.reason == "Send cancelled");
        REQUIRE(closeEvent.remote == false);

        // The server must not deliver the compressed prefix as a message.
        REQUIRE(waitForClose(serverRecorder));
        REQUIRE(serverRecorder.messageCount() == 0);

        client.stop();
        server.stop();
    }

    SECTION("A large message that compresses to a single frame cannot be cancelled")
    {
        int port = getFreePort();
        ix::WebSocketServer server(port);

        MessageRecorder serverRecorder;
        REQUIRE(startServer(server, serverRecorder));

        MessageRecorder clientRecorder;
        ix::WebSocket client;
        setupClient(client, port, clientRecorder);
        client.enablePerMessageDeflate();
        client.start();

        REQUIRE(waitForOpen(client));

        // Highly compressible: fits in a single compressed frame.
        std::string bigMessage(10 * 32768, 'd');

        WebSocketSendInfo info = client.sendText(bigMessage, [](int, int) { return false; });

        REQUIRE(info.success == true);
        REQUIRE(info.compressionError == false);
        REQUIRE(client.getReadyState() == ix::ReadyState::Open);

        for (int i = 0; i < 200 && serverRecorder.messageCount() == 0; ++i)
        {
            ix::msleep(10);
        }
        REQUIRE(serverRecorder.messageCount() == 1);
        REQUIRE(serverRecorder.lastMessage() == bigMessage);

        client.stop();
        server.stop();
    }

    SECTION("Reconnecting after a cancel starts from a clean state")
    {
        int port = getFreePort();
        ix::WebSocketServer server(port);

        MessageRecorder serverRecorder;
        REQUIRE(startServer(server, serverRecorder));

        MessageRecorder clientRecorder;
        ix::WebSocket client;
        setupClient(client, port, clientRecorder);
        client.enablePerMessageDeflate();
        client.start();

        REQUIRE(waitForOpen(client));

        std::string bigMessage = makeRandomString(10 * 32768);

        WebSocketSendInfo info = client.sendBinary(bigMessage, [](int, int) { return false; });
        REQUIRE(info.success == false);
        REQUIRE(waitForClose(clientRecorder));

        client.stop();

        // Reconnect with the same client object.
        clientRecorder.openCount = 0;
        client.start();
        REQUIRE(waitForOpen(client));

        // A new message sent on the new connection is delivered and
        // decompressed properly; no stale content shows up.
        std::string secondMessage = makeRandomString(100000);
        REQUIRE(client.sendBinary(secondMessage).success == true);

        for (int i = 0; i < 400 && serverRecorder.messageCount() == 0; ++i)
        {
            ix::msleep(10);
        }
        REQUIRE(serverRecorder.messageCount() == 1);
        REQUIRE(serverRecorder.lastMessage() == secondMessage);

        client.stop();
        server.stop();
    }
}

TEST_CASE("Websocket_send_cancel_server", "[send_cancel]")
{
    SECTION("A server can also cancel a large send")
    {
        int port = getFreePort();
        ix::WebSocketServer server(port);

        MessageRecorder serverRecorder;
        std::atomic<bool> serverSendDone {false};
        std::atomic<bool> serverSendSuccess {true};

        server.setOnClientMessageCallback(
            [&serverRecorder, &serverSendDone, &serverSendSuccess](
                std::shared_ptr<ConnectionState> /*connectionState*/,
                WebSocket& webSocket,
                const ix::WebSocketMessagePtr& msg) {
                if (msg->type == ix::WebSocketMessageType::Open)
                {
                    serverRecorder.openCount++;
                }
                else if (msg->type == ix::WebSocketMessageType::Close)
                {
                    serverRecorder.recordClose(
                        msg->closeInfo.code, msg->closeInfo.reason, msg->closeInfo.remote);
                }
                else if (msg->type == ix::WebSocketMessageType::Message)
                {
                    if (msg->str == "please send")
                    {
                        std::string bigMessage(10 * 32768, 'e');
                        WebSocketSendInfo info =
                            webSocket.sendText(bigMessage, [](int, int) { return false; });
                        serverSendSuccess = info.success;
                        serverSendDone = true;
                    }
                    else
                    {
                        serverRecorder.recordMessage(msg->str);
                    }
                }
            });

        auto res = server.listen();
        REQUIRE(res.first);
        server.start();

        MessageRecorder clientRecorder;
        ix::WebSocket client;
        setupClient(client, port, clientRecorder);
        client.start();

        REQUIRE(waitForOpen(client));
        REQUIRE(client.sendText("please send").success == true);

        // Wait for the server send to have been cancelled.
        for (int i = 0; i < 400 && !serverSendDone; ++i)
        {
            ix::msleep(10);
        }
        REQUIRE(serverSendDone == true);
        REQUIRE(serverSendSuccess == false);

        // The client gets a single close event, initiated by the server.
        REQUIRE(waitForClose(clientRecorder));
        REQUIRE(clientRecorder.closeEventCount() == 1);

        CloseEvent closeEvent = clientRecorder.lastCloseEvent();
        REQUIRE(closeEvent.code == 1000);
        REQUIRE(closeEvent.reason == "Send cancelled");
        REQUIRE(closeEvent.remote == true);

        // The cancelled message was never delivered.
        REQUIRE(clientRecorder.messageCount() == 0);

        client.stop();
        server.stop();
    }

    SECTION("A peer that never answers the close frame hits the closing delay")
    {
        int port = getFreePort();
        ix::WebSocketServer server(port);

        MessageRecorder serverRecorder;
        REQUIRE(startServer(server, serverRecorder));

        // Raw socket client: complete the handshake, then never read from
        // nor write to the socket again, so the close frame is never
        // answered.
        std::string errMsg;
        bool tls = false;
        SocketTLSOptions tlsOptions;
        std::shared_ptr<Socket> socket = createSocket(tls, -1, errMsg, tlsOptions);
        std::string host("127.0.0.1");
        auto isCancellationRequested = []() -> bool { return false; };
        REQUIRE(socket->connect(host, port, errMsg, isCancellationRequested));

        socket->writeBytes("GET / HTTP/1.1\r\n"
                           "Upgrade: websocket\r\n"
                           "Sec-WebSocket-Version: 13\r\n"
                           "Sec-WebSocket-Key: foobar\r\n"
                           "\r\n",
                           isCancellationRequested);

        // Read the 101 response, then go silent.
        auto lineResult = socket->readLine(isCancellationRequested);
        REQUIRE(lineResult.first);
        int status = -1;
        REQUIRE(sscanf(lineResult.second.c_str(), "HTTP/1.1 %d", &status) == 1);
        REQUIRE(status == 101);

        // Wait for the server to register the client.
        std::shared_ptr<WebSocket> serverClient;
        for (int i = 0; i < 200; ++i)
        {
            auto clients = server.getClients();
            if (clients.size() == 1)
            {
                serverClient = *clients.begin();
                break;
            }
            ix::msleep(10);
        }
        REQUIRE(serverClient != nullptr);

        // Cancel a large send right away. The peer does not read, so the
        // remaining bytes cannot be flushed.
        std::string bigMessage(10 * 32768, 'g');
        WebSocketSendInfo info =
            serverClient->sendText(bigMessage, [](int, int) { return false; });
        REQUIRE(info.success == false);
        REQUIRE(info.compressionError == false);

        // The peer never sends back a close frame: the existing closing
        // delay must terminate the connection anyway.
        REQUIRE(waitForClose(serverRecorder));
        REQUIRE(serverRecorder.closeEventCount() == 1);

        CloseEvent closeEvent = serverRecorder.lastCloseEvent();
        REQUIRE(closeEvent.code == 1000);
        REQUIRE(closeEvent.reason == "Send cancelled");
        REQUIRE(closeEvent.remote == false);

        // Nothing remains buffered once the connection is closed.
        REQUIRE(serverClient->bufferedAmount() == 0);

        socket->close();
        server.stop();
    }
}
