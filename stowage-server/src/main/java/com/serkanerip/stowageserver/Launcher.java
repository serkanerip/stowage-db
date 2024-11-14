package com.serkanerip.stowageserver;

public class Launcher {

    public static void main(String[] args) {
        var server = new StowageServer(ServerOptions.fromEnvironmentOrProperties());
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
    }

}
