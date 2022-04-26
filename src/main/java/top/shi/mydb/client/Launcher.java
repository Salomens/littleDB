package top.shi.mydb.client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import top.shi.mydb.transport.Encoder;
import top.shi.mydb.transport.Packager;
import top.shi.mydb.transport.Transporter;
//就是调Shell
public class Launcher {
    public static void main(String[] args) throws UnknownHostException, IOException {
        Socket socket = new Socket("127.0.0.1", 9999);
        Encoder e = new Encoder();
        Transporter t = new Transporter(socket);
        Packager packager = new Packager(t, e);

        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
