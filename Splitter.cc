#include <omnetpp.h>
#include <cstring>

using namespace omnetpp;

class Splitter : public cSimpleModule
{
  private:
    int numONUs = 0;

  protected:
    void initialize() override;
    void handleMessage(cMessage *msg) override;
};

Define_Module(Splitter);

void Splitter::initialize()
{
    numONUs = par("numONUs").intValue();
}

void Splitter::handleMessage(cMessage *msg)
{
    cGate *arrivalGate = msg->getArrivalGate();

    if (arrivalGate == nullptr)
    {
        delete msg;
        throw cRuntimeError("Splitter received a message without an arrival gate");
    }

    const char *arrivalGateName = arrivalGate->getName();

    // Normal upstream: ONU -> Splitter -> OLT.
    // Preserve the original DATA/REPORT packet.
    if (std::strcmp(arrivalGateName, "in") == 0)
    {
        send(msg, "out");
        return;
    }

    // Normal downstream: OLT -> Splitter -> ONU.
    // Broadcast DATA copies; each ONU checks destinationONU.
    if (std::strcmp(arrivalGateName, "downstreamIn") == 0)
    {
        for (int i = 0; i < numONUs; ++i)
            send(msg->dup(), "downstreamOut", i);

        delete msg;
        return;
    }

    // GATE/control path: broadcast to every ONU.
    if (std::strcmp(arrivalGateName, "grantIn") == 0)
    {
        for (int i = 0; i < numONUs; ++i)
            send(msg->dup(), "grantOut", i);

        delete msg;
        return;
    }

    // Local Sharing: reflect a copy to every ONU.
    if (std::strcmp(arrivalGateName, "lsIn") == 0)
    {
        for (int i = 0; i < numONUs; ++i)
            send(msg->dup(), "lsOut", i);

        delete msg;
        return;
    }

    delete msg;
    throw cRuntimeError(
        "Splitter received a message on unexpected gate '%s'",
        arrivalGateName);
}
