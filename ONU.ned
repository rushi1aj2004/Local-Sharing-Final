
simple ONU
{
    parameters:
        @display("i=device/router");

        // ONU identification
        int onuId;

        // Downstream traffic generation
        double packetInterval @unit(s) = default(10us);
        int packetSize @unit(b) = default(1000b);
        double dataRate @unit(bps) = default(10Gbps);

        // Upstream REPORT configuration
        int reportSizeBytes = default(64);

        // RN-to-ONU propagation delay, sampled once
        double rnToONUDelay @unit(s) = default(uniform(5us, 25us));

    gates:
        // Normal upstream DATA and REPORT transmission
        output out;
        input in;
        // Normal downstream GATE/control reception as well as DATA reception
        input grantIn;

        // Local Sharing wavelength
        output lsOut;
        input lsIn;
}
