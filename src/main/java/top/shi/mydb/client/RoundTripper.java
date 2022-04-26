package top.shi.mydb.client;

import top.shi.mydb.transport.Package;
import top.shi.mydb.transport.Packager;
//实现了单次收发动作
public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
