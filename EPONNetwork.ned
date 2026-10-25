
network EPONNetwork
{
    parameters:
        int numONUs = default(3);
        double del_OLTtoRN @unit(s) = default(75us);
        double dataRate @unit(bps) = default(10Gbps);
        double guardTime @unit(s) = default(1us);
        double maxCycleTime @unit(s) = default(1ms);
        int reportSizeBytes = default(64);

    submodules:
        olt: OLT {
            numONUs = parent.numONUs;
            maxCycleTime = parent.maxCycleTime;
            guardTime = parent.guardTime;
            reportSizeBytes = parent.reportSizeBytes;
        }

        splitter: Splitter {
            numONUs = parent.numONUs;
        }

        onu[numONUs]: ONU {
            onuId = index;
            dataRate = parent.dataRate;
            reportSizeBytes = parent.reportSizeBytes;
        }

        delayInitializer: DelayInitializer {
            numONUs = parent.numONUs;
        }

    connections:
        // Normal upstream: ONU -> Splitter
        for i = 0..numONUs-1 {
            onu[i].out --> EPONFiber --> splitter.in[i];
        }

        // Normal upstream: Splitter -> OLT
        splitter.out --> EPONFiber {
            propagationDelay = parent.del_OLTtoRN;
        } --> olt.in;

        // Normal downstream: OLT -> Splitter -> ONUs
        olt.grantOut --> EPONFiber {
            propagationDelay = parent.del_OLTtoRN;
        } --> splitter.grantIn;

        for i = 0..numONUs-1 {
            splitter.grantOut[i] --> EPONFiber --> onu[i].grantIn;
        }

        // Local Sharing: each ONU -> Splitter
        for i = 0..numONUs-1 {
            onu[i].lsOut --> EPONFiber --> splitter.lsIn[i];
        }

        // Local Sharing: Splitter -> every ONU
        for i = 0..numONUs-1 {
            splitter.lsOut[i] --> EPONFiber --> onu[i].lsIn;
        }
        olt.out --> EPONFiber {
            propagationDelay = parent.del_OLTtoRN;
        } --> splitter.downstreamIn;

        for i = 0..numONUs-1 {
            splitter.downstreamOut[i] --> EPONFiber {
                propagationDelay = uniform(5us, 25us);
            } --> onu[i].in;
        }
}
