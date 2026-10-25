
simple OLT
{
    parameters:
        @display("i=device/server");

        int numONUs;

        // Downstream parameters
        int maxGrantBytes = default(25000);
        int packetSize = default(1000);
        double packetInterval @unit(s) = default(10us);
        double dataRate @unit(bps) = default(10Gbps);
        double initialScheduleTime @unit(s) = default(20us);
        double t_tuning @unit(s) = default(1us);
        // Upstream scheduling parameters
        double maxCycleTime @unit(s) = default(400us);
        double guardTime @unit(s) = default(1us);
        int reportSizeBytes = default(64);

    gates:
        output out;
        // Upstream REPORT reception and GATE transmission
        input in;
        output grantOut;//Used for Downstreaming too
}
