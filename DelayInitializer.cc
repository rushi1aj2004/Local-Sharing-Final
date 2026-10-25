
#include <omnetpp.h>
#include <cmath>
#include <cstdint>

using namespace omnetpp;

class DelayInitializer : public cSimpleModule
{
  protected:
    void initialize() override;
};

Define_Module(DelayInitializer);

void DelayInitializer::initialize()
{
    cModule *network = getParentModule();

    int numONUs = par("numONUs").intValue();

    cModule *splitter = network->getSubmodule("splitter");

    for (int i = 0; i < numONUs; i++)
    {
        cModule *onu = network->getSubmodule("onu", i);

        // Read the sampled delay for this ONU.
        double delaySeconds =
            onu->par("rnToONUDelay").doubleValue();

        // Convert to integer picoseconds.
        int64_t delayPs = static_cast<int64_t>(
            std::llround(delaySeconds * 1e12)
        );

        simtime_t delay = SimTime(delayPs, SIMTIME_PS);

        // ------------------------------------
        // Normal upstream: ONU -> Splitter
        // ------------------------------------
        cChannel *dataChannel =
            onu->gate("out")->getChannel();
        
        // ------------------------------------
        // Normal downstream: Splitter -> ONU
        // ------------------------------------
        cChannel *grantChannel =
            splitter->gate("grantOut", i)->getChannel();

        // ------------------------------------
        // Local Sharing: ONU -> Splitter
        // ------------------------------------
        cChannel *lsUpstreamChannel =
            onu->gate("lsOut")->getChannel();

        // ------------------------------------
        // Local Sharing: Splitter -> ONU
        // ------------------------------------
        cChannel *lsDownstreamChannel =
            splitter->gate("lsOut", i)->getChannel();
        // ------------------------------------
        // Normal downstream DATA: Splitter -> ONU
        // ------------------------------------
        cChannel *downstreamDataChannel =
            splitter->gate("downstreamOut", i)->getChannel();
        // Apply the same sampled delay to all four channels.
        cChannel *channels[] = {
            dataChannel,
            grantChannel,
            lsUpstreamChannel,
            lsDownstreamChannel,
            downstreamDataChannel
        };

        for (cChannel *channel : channels)
        {
            channel->par("propagationDelay")
                   .setDoubleValue(delay.dbl());

            channel->par("delay")
                   .setDoubleValue(delay.dbl());
        }

        EV_INFO << "ONU " << i
                << " per-ONU propagation delay = "
                << delay
                << " (applied to normal and LS paths)"
                << endl;
    }
}
