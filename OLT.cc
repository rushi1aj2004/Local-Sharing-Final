
#include <omnetpp.h>
#include <vector>
#include <queue>
#include <algorithm>
#include <cmath>
#include <cstdint>

using namespace omnetpp;

class OLT : public cSimpleModule
{
  private:
    cMessage *scheduleMsg = nullptr;

    enum class SchedulerPhase
    {
        INITIAL_DISCOVERY,
        INITIAL_LOCAL_SHARING,
        STEADY_STATE
    };

    SchedulerPhase phase = SchedulerPhase::INITIAL_DISCOVERY;

    int numONUs = 0;
    bool initialScheduleDone = false;

    SimTime horizonUP = SIMTIME_ZERO;
    SimTime horizonLS = SIMTIME_ZERO;

    SimTime t_tuning;

    SimTime maxCycleTime;
    SimTime guardTime;

    double dataRate = 0;
    double oltToRNDelay = 0;

    long upMaxGrantBytes = 0;
    long reportSizeBytes = 0;
    long packetSizeBytes = 0;

    std::vector<long> queueBytes;
    std::vector<bool> isScheduled;
    std::vector<double> rnToONUDelays;

    std::vector<bool> initialReportReceived;
    int initialReportsRemaining = 0;

    int initialHostId = -1;
    std::vector<int> initialLSGroup;


    // Downstream scheduler state (kept independent of upstream/LS horizons).
    long downMaxGrantBytes = 0;
    long downstreamPacketSizeBits = 0;
    double downstreamPacketInterval = 0;
    SimTime horizonDown = SIMTIME_ZERO;
    SimTime downOltToSplitterDelay = SIMTIME_ZERO;
    SimTime downGuardTime = SIMTIME_ZERO;
    bool downstreamSchedulingStarted = false;
    int activeONU = -1;

    std::vector<std::queue<cPacket *>> currentBurst;
    std::vector<std::queue<cPacket *>> nextBurst;
    std::vector<cMessage *> generateMsg;
    std::vector<cMessage *> burstStartMsg;
    std::vector<bool> burstReserved;

    cMessage *downScheduleMsg = nullptr;
    cMessage *downTransmitMsg = nullptr;

    struct SplitterSlot {
        SimTime start;
        SimTime end;
    };
    std::vector<SplitterSlot> nextSlotAtSplitter;

    void sendGrantMessage(
        int onuId,
        long grantBytes,
        int mode,
        int hostId,
        double lsDuration,
        SimTime delayAfterGrant);

    void sendGrantForONU(int onuId, long grantBytes);

    void selectInitialLocalSharingGroup();
    void scheduleInitialLocalSharing();
    bool scheduleDynamicLocalSharing();
    

    long alignedGrantBytes(long bytes) const;
    bool reserveNextBurst(int onuId);
    void finishCurrentBurst();
    void startBurst(int onuId);
    void transmitNextPacket();
    int selectHost(
        const std::vector<int>& candidates,
        SimTime windowStart,
        SimTime windowEnd);
    bool hasDownstreamConflict(
        int candidateId,
        SimTime windowStart,
        SimTime windowEnd);
  protected:
    virtual void initialize() override;
    virtual void handleMessage(cMessage *msg) override;
    virtual void finish() override;
};

Define_Module(OLT);


static SimTime secondsToSimTime(double seconds)
{
    constexpr double TICKS_PER_SECOND = 1e12;

    int64_t ticks = static_cast<int64_t>(
        std::llround(seconds * TICKS_PER_SECOND)
    );

    return SimTime(ticks, SIMTIME_PS);
}


long OLT::alignedGrantBytes(long bytes) const
{
    if (bytes <= 0)
        return 0;

    bytes = std::min(bytes, upMaxGrantBytes);
    bytes -= bytes % packetSizeBytes;

    return std::max(0L, bytes);
}


void OLT::initialize()
{
    scheduleMsg = new cMessage("InitialSchedule");

    cModule *network = getParentModule();

    numONUs = par("numONUs").intValue();

    if (numONUs < 6)
        throw cRuntimeError(
            "Initial Local Sharing requires at least 6 ONUs"
        );

    maxCycleTime = par("maxCycleTime");
    guardTime = par("guardTime");

    reportSizeBytes = par("reportSizeBytes").intValue();

    dataRate = network->par("dataRate").doubleValue();

    oltToRNDelay =
        network->par("del_OLTtoRN").doubleValue();

    t_tuning = par("t_tuning");

    if (dataRate <= 0 ||
        reportSizeBytes <= 0 ||
        guardTime < SIMTIME_ZERO ||
        maxCycleTime <= SIMTIME_ZERO)
    {
        throw cRuntimeError("Invalid OLT scheduling parameters");
    }

    rnToONUDelays.resize(numONUs);

    cModule *firstONU = network->getSubmodule("onu", 0);

    long packetSizeBits =
        firstONU->par("packetSize").intValue();

    if (packetSizeBits <= 0 || packetSizeBits % 8 != 0)
        throw cRuntimeError(
            "packetSize must be a positive multiple of 8 bits"
        );

    packetSizeBytes = packetSizeBits / 8;

    for (int i = 0; i < numONUs; i++)
    {
        cModule *onuModule = network->getSubmodule("onu", i);

        rnToONUDelays[i] =
            onuModule->par("rnToONUDelay").doubleValue();

        EV_INFO << "ONU " << i
                << " per-ONU delay = "
                << rnToONUDelays[i] * 1e6
                << " us"
                << endl;
    }

    // Calculate a conservative normal grant size.
    double reportTxTime =
        reportSizeBytes * 8.0 / dataRate;

    double usableDataTime =
        maxCycleTime.dbl()
        - numONUs * reportTxTime
        - (numONUs - 1) * guardTime.dbl();

    if (usableDataTime <= 0)
        throw cRuntimeError("Cycle time is too short");

    upMaxGrantBytes = static_cast<long>(
        std::floor(
            dataRate * usableDataTime / (8.0 * numONUs)
        )
    );

    upMaxGrantBytes -= upMaxGrantBytes % packetSizeBytes;

    if (upMaxGrantBytes < packetSizeBytes)
        throw cRuntimeError("Calculated grant is too small");

    queueBytes.assign(numONUs, 0);
    isScheduled.assign(numONUs, false);
    initialReportReceived.assign(numONUs, false);
    initialReportsRemaining = numONUs;

    horizonUP = SIMTIME_ZERO;
    horizonLS = SIMTIME_ZERO;

    phase = SchedulerPhase::INITIAL_DISCOVERY;

    EV_INFO << "\n=== OLT CONFIGURATION ==="
            << "\nNumber of ONUs = " << numONUs
            << "\nData rate = " << dataRate
            << " bit/s"
            << "\nMaximum grant = " << upMaxGrantBytes
            << " bytes"
            << "\nREPORT size = " << reportSizeBytes
            << " bytes"
            << "\nGuard time = " << guardTime
            << endl;



    // Initialize the independent downstream scheduler.
    downMaxGrantBytes = par("maxGrantBytes").intValue();
    downstreamPacketSizeBits = par("packetSize").intValue();
    downstreamPacketInterval = par("packetInterval").doubleValue();

    if (downMaxGrantBytes <= 0 ||
        downstreamPacketSizeBits <= 0 ||
        downstreamPacketInterval <= 0)
        throw cRuntimeError(
            "Downstream grant, packet size, and packet interval must be positive");

    downOltToSplitterDelay =
        secondsToSimTime(network->par("del_OLTtoRN").doubleValue());

    // Preserve the original Downstream OLT's guard interval.
    downGuardTime = SimTime(10, SIMTIME_NS);

    currentBurst.resize(numONUs);
    nextBurst.resize(numONUs);
    generateMsg.resize(numONUs, nullptr);
    burstStartMsg.resize(numONUs, nullptr);
    burstReserved.resize(numONUs, false);
    nextSlotAtSplitter.resize(numONUs);
    activeONU = -1;

    horizonDown = par("initialScheduleTime");
    downScheduleMsg = new cMessage("InitialDownstreamSchedule");
    downTransmitMsg = new cMessage("DownstreamTransmitPacket");

    for (int i = 0; i < numONUs; ++i) {
        generateMsg[i] = new cMessage("DownstreamGeneratePacket");
        burstStartMsg[i] = new cMessage("DownstreamStartBurst");
        scheduleAt(simTime(), generateMsg[i]);
    }

    scheduleAt(horizonDown, downScheduleMsg);


        scheduleAt(SIMTIME_ZERO, scheduleMsg);
}


bool OLT::hasDownstreamConflict(
    int candidateId,
    SimTime windowStart,
    SimTime windowEnd)
{
    if (candidateId < 0 ||
        candidateId >= static_cast<int>(nextSlotAtSplitter.size()))
    {
        throw cRuntimeError(
            "Invalid candidate ONU ID: %d", candidateId);
    }

    const SplitterSlot& slot = nextSlotAtSplitter[candidateId];

    // Half-open intervals: [start, end)
    return slot.start < windowEnd &&
           windowStart < slot.end;
}

int OLT::selectHost(const std::vector<int>& candidates,SimTime windowStart,SimTime windowEnd)
{
    int hostId = -1;
    long largestEligibleBurst = -1;

    for (int candidateId : candidates)
    {
        if (isScheduled[candidateId])
            continue;

        if (hasDownstreamConflict(
            candidateId, windowStart, windowEnd))
        {
            continue;
        }

        const long eligibleBurst =
        alignedGrantBytes(queueBytes[candidateId]);

        if (eligibleBurst > largestEligibleBurst ||
        (eligibleBurst == largestEligibleBurst &&
        (hostId < 0 || candidateId < hostId)))
        {
        largestEligibleBurst = eligibleBurst;
        hostId = candidateId;
        }
    }

return hostId;
}
// Create and send a GATE. Timing and reservations are calculated
// by the caller so LS and UP paths can be scheduled independently.
void OLT::sendGrantMessage(
    int onuId,
    long grantBytes,
    int mode,
    int hostId,
    double lsDuration,
    SimTime delayAfterGrant)
{
    if (onuId < 0 || onuId >= numONUs)
        throw cRuntimeError("Invalid ONU ID in GATE");

    if (grantBytes < 0 || delayAfterGrant < SIMTIME_ZERO)
        throw cRuntimeError("Invalid GATE grant or delay");

    cMessage *grant = new cMessage("Grant");

    grant->addPar("onuId");
    grant->par("onuId").setLongValue(onuId);

    grant->addPar("grantBytes");
    grant->par("grantBytes").setLongValue(grantBytes);

    grant->addPar("delay_after_rec_grant");
    grant->par("delay_after_rec_grant")
         .setDoubleValue(delayAfterGrant.dbl());

    grant->addPar("mode");
    grant->par("mode").setLongValue(mode);

    grant->addPar("hostId");
    grant->par("hostId").setLongValue(hostId);

    grant->addPar("lsDuration");
    grant->par("lsDuration").setDoubleValue(lsDuration);

    EV_INFO << "\nOLT sends GATE"
            << "\nONU = " << onuId
            << "\nMode = " << mode
            << "\nHost ID = " << hostId
            << "\nGrant = " << grantBytes << " bytes"
            << "\nDelay after GATE reception = "
            << delayAfterGrant.dbl() * 1e6 << " us"
            << "\nLS duration = " << lsDuration * 1e6 << " us"
            << endl;

    send(grant, "grantOut");
}


// Normal upstream scheduling. The reservation horizon represents
// the expected end of the previous UP reservation at the OLT.
void OLT::sendGrantForONU(int onuId, long grantBytes)
{
    grantBytes = alignedGrantBytes(grantBytes);

    SimTime upstreamPropagation =
        secondsToSimTime(
            oltToRNDelay + rnToONUDelays[onuId]
        );

    SimTime roundTrip =
        secondsToSimTime(
            2.0 * (oltToRNDelay + rnToONUDelays[onuId])
        );

    SimTime earliestArrival = simTime() + roundTrip;

    SimTime arrivalTime =
        std::max(horizonUP, earliestArrival);

    SimTime delayAfterGrant =
        arrivalTime - simTime() - roundTrip;

    sendGrantMessage(
        onuId,
        grantBytes,
        0,
        -1,
        0.0,
        delayAfterGrant
    );

    SimTime dataTxTime =
        secondsToSimTime(grantBytes * 8.0 / dataRate);

    SimTime reportTxTime =
        secondsToSimTime(reportSizeBytes * 8.0 / dataRate);

    horizonUP =
        arrivalTime + dataTxTime + reportTxTime + guardTime;

    EV_INFO << "UP reservation: ONU " << onuId
            << ", first arrival = " << arrivalTime
            << ", reserved until = " << horizonUP
            << ", upstream propagation = " << upstreamPropagation
            << endl;
}


// Rank ONUs and select the six largest reported queues.
void OLT::selectInitialLocalSharingGroup()
{
    if (phase != SchedulerPhase::INITIAL_DISCOVERY)
        return;

    if (initialReportsRemaining != 0)
        throw cRuntimeError(
            "Initial discovery is not complete"
        );

    std::vector<int> rankedONUs(numONUs);

    for (int i = 0; i < numONUs; i++)
        rankedONUs[i] = i;

    std::stable_sort(
        rankedONUs.begin(),
        rankedONUs.end(),
        [this](int a, int b)
        {
            return queueBytes[a] > queueBytes[b];
        }
    );

    initialLSGroup.assign(
        rankedONUs.begin(),
        rankedONUs.begin() + 6
    );

    // The highest reported queue becomes the Host.
    initialHostId = initialLSGroup.front();

    

    EV_INFO << "\n=== INITIAL LOCAL SHARING GROUP ==="
            << "\nHost ONU = " << initialHostId
            << "\nReported queue = "
            << queueBytes[initialHostId] << " bytes"
            << endl;

    for (int rank = 0; rank < 6; rank++)
    {
        int onuId = initialLSGroup[rank];

        EV_INFO << "Rank " << rank + 1
                << ": ONU " << onuId
                << ", queue = " << queueBytes[onuId]
                << " bytes"
                << (rank == 0 ? " (HOST)" : " (CONTRIBUTOR)")
                << endl;
    }

    phase = SchedulerPhase::INITIAL_LOCAL_SHARING;
    scheduleAt(simTime(), scheduleMsg);
}


// Schedule five contributors sequentially on the LS path,
// then schedule the Host after their packets can arrive.
void OLT::scheduleInitialLocalSharing()
{
    if (phase != SchedulerPhase::INITIAL_LOCAL_SHARING)
        return;

    SimTime now = simTime();

    SimTime lsCursor = now;
    SimTime latestLSArrivalAtHost = now;

    long expectedForwardBytes = 0;

    SimTime reportTxTime =
        secondsToSimTime(reportSizeBytes * 8.0 / dataRate);

    // A Mode 1 contributor sends its DATA and then its REPORT
    // through the Local Sharing path.
    for (int rank = 1; rank < 6; rank++)
    {
        int contributorId = initialLSGroup[rank];

        long grantBytes =
            alignedGrantBytes(queueBytes[contributorId]);

        SimTime dataTxTime =
            secondsToSimTime(grantBytes * 8.0 / dataRate);

        SimTime contributorDelay =
            secondsToSimTime(rnToONUDelays[contributorId]);

        SimTime hostDelay =
            secondsToSimTime(rnToONUDelays[initialHostId]);

        // Earliest arrival at the Splitter if the contributor
        // starts as soon as its broadcast GATE reaches it.
        SimTime earliestSplitterArrival =
            now
            + secondsToSimTime(oltToRNDelay)
            + contributorDelay
            + contributorDelay;

        SimTime lsStartAtSplitter =
            std::max(lsCursor, earliestSplitterArrival);

        // GATE reaches ONU after OLT->Splitter and Splitter->ONU.
        // DATA then travels ONU->Splitter.
        SimTime delayAfterGrant =
            lsStartAtSplitter
            - now
            - secondsToSimTime(oltToRNDelay)
            - contributorDelay
            - contributorDelay;

        if (delayAfterGrant < SIMTIME_ZERO)
            delayAfterGrant = SIMTIME_ZERO;

        SimTime contributorBurstDuration =
            dataTxTime + reportTxTime;

        sendGrantMessage(
            contributorId,
            grantBytes,
            1,
            initialHostId,
            contributorBurstDuration.dbl(),
            delayAfterGrant
        );

        // The Host receives the reflected packets after the
        // Splitter->Host propagation delay.
        SimTime reportArrivalAtHost =
            lsStartAtSplitter
            + contributorBurstDuration
            + hostDelay;

        latestLSArrivalAtHost =
            std::max(latestLSArrivalAtHost, reportArrivalAtHost);

        // Reserve the LS wavelength through DATA + REPORT + guard.
        lsCursor =
            lsStartAtSplitter
            + contributorBurstDuration
            + guardTime;

        // Upper bound on bytes the Host must forward.
        expectedForwardBytes += grantBytes + reportSizeBytes;

        EV_INFO << "\nScheduled LS contributor " << contributorId
                << "\nHost = " << initialHostId
                << "\nGrant bytes = " << grantBytes
                << "\nLS start at Splitter = " << lsStartAtSplitter
                << "\nExpected REPORT arrival at Host = "
                << reportArrivalAtHost
                << "\nLS reservation end = " << lsCursor
                << endl;
    }

    horizonLS = lsCursor;

    // The Host can start only after its GATE arrives and all
    // scheduled contributor packets can have reached its LS buffer.
    SimTime hostGateArrival =
        now
        + secondsToSimTime(oltToRNDelay)
        + secondsToSimTime(rnToONUDelays[initialHostId]);

    SimTime hostSendStart =
        std::max(hostGateArrival, latestLSArrivalAtHost);

    //Update HorizonUP to make a gap of 125 us between the last contributor's report and the data arrival at OLT from the Host.NEWADDITION
    horizonUP=latestLSArrivalAtHost + SimTime(125, SIMTIME_US);
    // Keep the UP path collision-free as well.
    SimTime hostUpstreamPropagation =
        secondsToSimTime(
            oltToRNDelay + rnToONUDelays[initialHostId]
        );

    if (hostSendStart + hostUpstreamPropagation < horizonUP)
    {
        hostSendStart = horizonUP - hostUpstreamPropagation;

        if (hostSendStart < hostGateArrival)
            hostSendStart = hostGateArrival;
    }

    SimTime hostDelayAfterGrant =
        hostSendStart - hostGateArrival;
    //EV_INFO << "Host GATE arrives at " << hostDelayAfterGrant << " us after GATE reception" << endl;
    if (hostDelayAfterGrant < SIMTIME_ZERO)
        hostDelayAfterGrant = SIMTIME_ZERO;

    // Mode 2 grant covers only the Host's own DATA.
    // Forwarded packets are accounted for separately in time.
    long hostGrantBytes =
        alignedGrantBytes(queueBytes[initialHostId]);

    SimTime hostOwnDataTime =
        secondsToSimTime(hostGrantBytes * 8.0 / dataRate);

    // The Host forwards contributor DATA and REPORT packets
    // sequentially, before sending its own DATA and REPORT.
    SimTime forwardingTime =
        secondsToSimTime(expectedForwardBytes * 8.0 / dataRate);

    SimTime hostFirstArrivalAtOLT =
        hostSendStart + hostUpstreamPropagation;

    horizonUP =
        hostFirstArrivalAtOLT
        + forwardingTime
        + hostOwnDataTime
        + reportTxTime
        + guardTime;

    SimTime totalInitialLSDuration =
        horizonLS - now;

    sendGrantMessage(
        initialHostId,
        hostGrantBytes,
        2,
        -1,
        totalInitialLSDuration.dbl(),
        hostDelayAfterGrant
    );

    // The initial reports for the committed Host/group are consumed.
    isScheduled[initialHostId] = true;
    for (int rank = 1; rank < 6; rank++)
        isScheduled[initialLSGroup[rank]] = true;

    EV_INFO << "\n=== INITIAL SCHEDULING COMPLETE ==="
            << "\nNext LS scheduling time = "
            << horizonLS - SimTime(125, SIMTIME_US)
            << "\nUP horizon = " << horizonUP
            << "\nLS horizon = " << horizonLS
            << endl;
    EV_INFO << "\n=== INITIAL LOCAL SHARING SCHEDULED ==="
            << "\nHost = " << initialHostId
            << "\nHost own grant = " << hostGrantBytes << " bytes"
            << "\nExpected forwarded bytes = "
            << expectedForwardBytes
            << "\nExpected Host start = " << hostSendStart
            << "\nLatest contributor REPORT at Host = "
            << latestLSArrivalAtHost
            << "\nLS horizon = " << horizonLS
            << "\nUP horizon = " << horizonUP
            << endl;
}


// Dynamic upstream-only Local Sharing scheduler.
// Returns true only when an LS group reservation was committed.
bool OLT::scheduleDynamicLocalSharing()
{
    const SimTime jitOffset = SimTime(125, SIMTIME_US);
    const SimTime now = simTime();
    const SimTime reportTxTime =
        secondsToSimTime(reportSizeBytes * 8.0 / dataRate);

    
    SimTime maxDownstreamEnd = SIMTIME_ZERO;

    for (const auto& slot : nextSlotAtSplitter)
    {
        if (slot.end > maxDownstreamEnd)
            maxDownstreamEnd = slot.end;
    }

    //The LS window is measured at the Splitter/RN.
    SimTime rightLimit = std::min(horizonUP - jitOffset,maxDownstreamEnd);//right limit of the LS window is the minimum of the UP horizon and the end of the last downstream burst at the Splitter
    SimTime deltaTLS = rightLimit - horizonLS;

    EV_INFO << "\nDynamic LS window"
            << "\nRight limit = " << rightLimit
            << "\nDelta_T_LS = " << deltaTLS
            << endl;

    // Stage A: if the window is too small, schedule ordinary upstream
    // grants from fresh, unconsumed REPORTs to advance Horizon_UP.
    // Each fallback ONU is consumed only once in this scheduling round.
    while (deltaTLS <= 2 * guardTime)
    {
        int fallbackONU = -1;
        long largestQueue = -1;

        for (int i = 0; i < numONUs; ++i)
        {
            if (isScheduled[i])
                continue;

            if (queueBytes[i] > largestQueue)
            {
                largestQueue = queueBytes[i];
                fallbackONU = i;
            }
        }

        if (fallbackONU < 0)
        {
            EV_INFO << "No fresh REPORT available for fallback upstream scheduling; "
                    << "deferring LS until a new REPORT arrives."
                    << endl;
            return false;
        }

        long fallbackGrant = alignedGrantBytes(queueBytes[fallbackONU]);
        isScheduled[fallbackONU] = true;

        EV_INFO << "LS window too small; fallback upstream grant for ONU "
                << fallbackONU << ", grant = " << fallbackGrant
                << " bytes" << endl;

        sendGrantForONU(fallbackONU, fallbackGrant);

        rightLimit = horizonUP - jitOffset;
        deltaTLS = rightLimit - horizonLS;
    }

    if (deltaTLS <= 2 * guardTime)
    {
        EV_INFO << "No usable LS window; waiting for a fresh REPORT."
                << endl;
        return false;
    }

    // Stage B: eligible Host candidates are fresh, unconsumed reports
    // with at least one complete DATA packet available for a grant.
    std::vector<int> candidates;
    candidates.reserve(numONUs);

    for (int i = 0; i < numONUs; ++i)
    {
        if (!isScheduled[i] && alignedGrantBytes(queueBytes[i]) >= 0)
            candidates.push_back(i);
    }

    std::stable_sort(candidates.begin(), candidates.end(),
        [this](int a, int b) {
            if (queueBytes[a] != queueBytes[b])
                return queueBytes[a] > queueBytes[b];
            return a < b;
        });

        if (candidates.size() < 2)
        {
            if (!candidates.empty())
            {
                const int onlyONU = candidates.front();
                const long grantBytes = alignedGrantBytes(queueBytes[onlyONU]);
        
                // Available time for Local Sharing at the Splitter.
                SimTime deltaTLS =
                    horizonUP
                    - SimTime(125, SIMTIME_US)
                    - horizonLS;
        
                EV_INFO << "Only one ONU has eligible DATA"
                        << "\nONU = " << onlyONU
                        << "\nGrant = " << grantBytes << " bytes"
                        << "\nDelta_T_LS = " << deltaTLS
                        << endl;
        
                // Case 1: Wait for another opportunity to form an LS group.
                if (deltaTLS > 3 * guardTime)
                {
                    SimTime waitTime = deltaTLS / 4;
        
                    // Advance the LS horizon by one-quarter of the
                    // currently available LS window.
                    horizonLS += waitTime;
        
                    // Consume this scheduling decision without consuming
                    // the ONU's REPORT: it may participate in LS later.
                    SimTime nextDecision =
                        horizonLS - SimTime(125, SIMTIME_US);
        
                    // Do not schedule the next event in the past.
                    if (nextDecision < simTime())
                    {
                        nextDecision = simTime();
                        horizonLS =
                            nextDecision + SimTime(125, SIMTIME_US);
                    }
        
                    EV_INFO << "LS window is large enough to wait"
                            << "\nWait time = " << waitTime
                            << "\nUpdated Horizon_LS = " << horizonLS
                            << "\nNext scheduling time = " << nextDecision
                            << endl;
        
                    if (!scheduleMsg->isScheduled())
                        scheduleAt(nextDecision, scheduleMsg);
        
                    return false;
                }
        
                // Case 2: Insufficient LS opportunity; serve this ONU
                // through ordinary upstream scheduling.
                isScheduled[onlyONU] = true;
        
                EV_INFO << "LS window too small; using normal upstream "
                        << "grant for ONU " << onlyONU
                        << ", grant = " << grantBytes << " bytes"
                        << endl;
        
                sendGrantForONU(onlyONU, grantBytes);
            }
            else
            {
                EV_INFO << "No ONU has eligible DATA; waiting for a fresh REPORT."
                        << endl;
            }
        
            return false;
        }

    // Upstream-only phase: choose the maximum eligible burst as Host.
    // If eligible bursts tie (e.g. both are capped at upMaxGrantBytes),
    // choose the smallest ONU ID deterministically. Contributor order
    // remains decreasing reported queue size as requested.
    std::priority_queue<std::pair<SimTime, int>> downstreamEndHeap;
    for (int candidateId : candidates)
    {
        const SplitterSlot& slot = nextSlotAtSplitter[candidateId];

        downstreamEndHeap.push(
            std::make_pair(slot.end, candidateId)
        );
    }

    int hostId = -1;

    while (rightLimit > horizonLS)
    {
        hostId = selectHost(
            candidates,
            horizonLS - t_tuning,
            rightLimit + t_tuning);

        if (hostId >= 0)
            break;

        // Need at least two finish times to use the
        // second-latest finish as the revised right limit.
        if (downstreamEndHeap.size() < 2)
            break;

        // Remove the latest downstream burst finish.
        downstreamEndHeap.pop();

        // The new top is the second-latest remaining finish.
        SimTime revisedRight = downstreamEndHeap.top().first;

        if (revisedRight >= rightLimit)
            break; // No progress; avoid an infinite loop.
                    //We are not considering the case where start may be less that rightLimit and end is greater than rightLimit.Because that is only possible for the last candidate which was already popped from the heap and we are not considering that candidate anymore.
        rightLimit = revisedRight - t_tuning;
        deltaTLS = rightLimit - horizonLS;
    }

    deltaTLS = rightLimit - horizonLS;

    if (deltaTLS < 2 * guardTime)
    {
        EV_INFO << "LS window became too small after adjusting "
                << "the right limit for tuning time."
                << "\nDelta_T_LS = " << deltaTLS
                << "\nScheduling all available candidates "
                << "through normal upstream grants."
                << endl;

        for (int candidateId : candidates)
        {
            if (isScheduled[candidateId])
                continue;

            long grantBytes =
                alignedGrantBytes(queueBytes[candidateId]);

            isScheduled[candidateId] = true;
            sendGrantForONU(candidateId, grantBytes);
        }

        return false;
    }


    if (hostId < 0)
    {
        if (!downstreamEndHeap.empty())
        {
            // Get the end time of the last remaining downstream slot.
            SimTime lastBurstEnd = downstreamEndHeap.top().first;

            // Advance Horizon_LS past the downstream activity
            // and allow time for receiver wavelength tuning.
            horizonLS = lastBurstEnd + t_tuning;

            // Schedule the next scheduling decision using the
            // existing just-in-time scheduling rule.
            SimTime nextDecision =
                horizonLS - SimTime(125, SIMTIME_US);

            if (nextDecision < simTime())
                nextDecision = simTime();

            EV_INFO << "No feasible Host found."
                    << "\nLast downstream burst end = " << lastBurstEnd
                    << "\nUpdated Horizon_LS = " << horizonLS
                    << "\nNext scheduling time = " << nextDecision
                    << endl;

            if (!scheduleMsg->isScheduled())
                scheduleAt(nextDecision, scheduleMsg);
        }
        else
        {
            EV_INFO << "No feasible Host and no downstream slots remain. "
                    << "Waiting for a fresh REPORT."
                    << endl;
        }

        return false;
    }

    const SimTime oldHorizonLS = horizonLS;
    SimTime lsCursor = horizonLS;
    SimTime latestLSArrivalAtHost = horizonLS;
    long expectedForwardBytes = 0;
    std::vector<int> committedContributors;

    const SimTime oltToRNSim = secondsToSimTime(oltToRNDelay);
    const SimTime hostDelay = secondsToSimTime(rnToONUDelays[hostId]);

    EV_INFO << "\nDynamic LS Host selected: ONU " << hostId
            << ", reported queue = " << queueBytes[hostId]
            << " bytes" << endl;

    // Stage C: process contributors in decreasing queue order and fit
    // each DATA + REPORT + guard slot into the remaining LS window.
    for (int contributorId : candidates)
    {
        if (contributorId == hostId)
            continue;
        const SimTime contributorDelay =
            secondsToSimTime(rnToONUDelays[contributorId]);

        // GATE: OLT->RN->ONU; DATA: ONU->RN.
        const SimTime earliestSplitterArrival =
            now + oltToRNSim + contributorDelay + contributorDelay;
        const SimTime lsStartAtSplitter =
            std::max(lsCursor, earliestSplitterArrival);

        const SimTime remainingAtSplitter = rightLimit - lsStartAtSplitter;
        const SimTime usableDataTime =
            remainingAtSplitter - reportTxTime - guardTime;

        if (usableDataTime <= SIMTIME_ZERO)
        {
            EV_INFO << "No time left for another LS contributor; stopping group fit."
                    << endl;
            break;
        }

        long timeCapacityBytes = static_cast<long>(
            std::floor(dataRate * usableDataTime.dbl() / 8.0));
        timeCapacityBytes -= timeCapacityBytes % packetSizeBytes;

        const long grantBytes = std::min(
            alignedGrantBytes(queueBytes[contributorId]),
            alignedGrantBytes(timeCapacityBytes));

        if (grantBytes < 0)
            continue;

        SimTime delayAfterGrant =
            lsStartAtSplitter - now - oltToRNSim
            - contributorDelay - contributorDelay;
        if (delayAfterGrant < SIMTIME_ZERO)
            delayAfterGrant = SIMTIME_ZERO;

        const SimTime dataTxTime =
            secondsToSimTime(grantBytes * 8.0 / dataRate);
        const SimTime contributorSlotDuration =
            dataTxTime + reportTxTime;

        // Commit the contributor's REPORT to this LS reservation.
        isScheduled[contributorId] = true;
        committedContributors.push_back(contributorId);

        sendGrantMessage(
            contributorId,
            grantBytes,
            1,
            hostId,
            contributorSlotDuration.dbl(),
            delayAfterGrant);

        const SimTime reportArrivalAtHost =
            lsStartAtSplitter + contributorSlotDuration + hostDelay;
        latestLSArrivalAtHost =
            std::max(latestLSArrivalAtHost, reportArrivalAtHost);

        expectedForwardBytes += grantBytes + reportSizeBytes;
        lsCursor = lsStartAtSplitter + contributorSlotDuration + guardTime;

        EV_INFO << "Dynamic LS contributor ONU " << contributorId
                << "\nGrant = " << grantBytes << " bytes"
                << "\nLS start at Splitter = " << lsStartAtSplitter
                << "\nSlot duration (DATA + REPORT) = "
                << contributorSlotDuration
                << "\nLS reservation end = " << lsCursor
                << endl;
    }

    // A Host-only reservation is not Local Sharing. Leave its REPORT
    // available if no contributor could fit in this LS window.
    if (committedContributors.empty())
    {
        EV_INFO << "No contributor fitted into the LS window; "
                << "no LS reservation committed."
                << endl;
        return false;
    }

    // Stage D: reserve the Host's aggregate upstream burst. The Host's
    // individual GATE allowance covers only its own DATA; contributor
    // DATA/REPORT serialization is reserved separately in time.
    const SimTime hostGateArrival =
        now + oltToRNSim + hostDelay;
    SimTime hostSendStart =
        std::max(hostGateArrival, latestLSArrivalAtHost);
    const SimTime hostUpstreamPropagation = oltToRNSim + hostDelay;

    if (hostSendStart + hostUpstreamPropagation < horizonUP)
        hostSendStart = horizonUP - hostUpstreamPropagation;
    if (hostSendStart < hostGateArrival)
        hostSendStart = hostGateArrival;

    SimTime hostDelayAfterGrant = hostSendStart - hostGateArrival;
    if (hostDelayAfterGrant < SIMTIME_ZERO)
        hostDelayAfterGrant = SIMTIME_ZERO;

    const long hostGrantBytes = alignedGrantBytes(queueBytes[hostId]);
    const SimTime hostOwnDataTime =
        secondsToSimTime(hostGrantBytes * 8.0 / dataRate);
    const SimTime forwardingTime =
        secondsToSimTime(expectedForwardBytes * 8.0 / dataRate);
    const SimTime hostFirstArrivalAtOLT =
        hostSendStart + hostUpstreamPropagation;

    horizonLS = lsCursor;
    horizonUP = hostFirstArrivalAtOLT
        + forwardingTime
        + hostOwnDataTime
        + reportTxTime
        + guardTime;

    // Consume the Host REPORT only when the aggregate reservation is committed.
    isScheduled[hostId] = true;

    const SimTime totalLSDuration = horizonLS - oldHorizonLS;
    sendGrantMessage(
        hostId,
        hostGrantBytes,
        2,
        -1,
        totalLSDuration.dbl(),
        hostDelayAfterGrant);

    EV_INFO << "\n=== DYNAMIC LS RESERVATION COMMITTED ==="
            << "\nHost ONU = " << hostId
            << "\nContributors = " << committedContributors.size()
            << "\nExpected forwarded bytes = " << expectedForwardBytes
            << "\nHost own grant = " << hostGrantBytes << " bytes"
            << "\nHorizon_LS: " << oldHorizonLS << " -> " << horizonLS
            << "\nHorizon_UP = " << horizonUP
            << "\nNext JIT time = " << horizonLS - jitOffset
            << endl;

    return true;
}


bool OLT::reserveNextBurst(int onuId)
{
    if (burstReserved[onuId])
        return false;

    long long grantBits =
        static_cast<long long>(downMaxGrantBytes) * 8LL;

    long long selectedBits = 0;

    // Select packets from nextBurst without exceeding the grant.
    // Packets that do not fit remain in nextBurst.
    while (!nextBurst[onuId].empty())
    {
        cPacket *packet = nextBurst[onuId].front();

        long long packetBits = packet->getBitLength();

        if (selectedBits + packetBits > grantBits)
            break;

        currentBurst[onuId].push(packet);
        nextBurst[onuId].pop();

        selectedBits += packetBits;
    }

    // No packet fits the grant.
    if (currentBurst[onuId].empty())
        return false;

    // Never reserve a burst in the past.
    if (horizonDown < simTime())
        horizonDown = simTime();

    // Calculate actual burst start and end.
    SimTime burstStart = horizonDown;

    SimTime burstTime(
        static_cast<double>(selectedBits) / dataRate);

    SimTime burstEnd = burstStart + burstTime;

    // Splitter interval includes propagation delay only.
    SimTime oltToSplitterDelay = downOltToSplitterDelay;

    nextSlotAtSplitter[onuId].start =
        burstStart + oltToSplitterDelay;

    nextSlotAtSplitter[onuId].end =
        burstEnd + oltToSplitterDelay;

    // Guard time is separate from the actual burst interval.
    SimTime guardTime = downGuardTime;

    // Advance the reservation horizon beyond the burst and guard.
    horizonDown = burstEnd + guardTime;

    burstReserved[onuId] = true;

    EV << "Reserved downstream burst for ONU " << onuId
       << "\n  OLT burst start = " << burstStart
       << "\n  OLT burst end = " << burstEnd
       << "\n  Actual burst duration = " << burstTime
       << "\n  Guard time = " << guardTime
       << "\n  Splitter slot = ["
       << nextSlotAtSplitter[onuId].start << ", "
       << nextSlotAtSplitter[onuId].end << "]"
       << "\n  Updated horizonDown = " << horizonDown
       << "\n";

    // The burst will start at its reserved absolute time.
    scheduleAt(burstStart, burstStartMsg[onuId]);

    return true;
}

void OLT::startBurst(int onuId)
{
    if (activeONU != -1)
    {
        throw cRuntimeError(
            "ONU %d attempted to start while ONU %d is active",
            onuId, activeONU);
    }

    activeONU = onuId;

    EV << "Starting downstream burst for ONU "
       << onuId << " at " << simTime()
       << ", packets = " << currentBurst[onuId].size()
       << "\n";

    // The shared downTransmitMsg should not already be scheduled here.
    if (downTransmitMsg->isScheduled())
    {
        throw cRuntimeError(
            "TransmitPacket was already scheduled when ONU %d started",
            onuId);
    }

    scheduleAt(simTime(), downTransmitMsg);
}

void OLT::transmitNextPacket()
{
    if (activeONU < 0)
        return;

    int onuId = activeONU;

    // The previous packet's transmission has completed and
    // there are no packets remaining in this burst.
    if (currentBurst[onuId].empty())
    {
        finishCurrentBurst();
        return;
    }

    cPacket *packet = currentBurst[onuId].front();
    currentBurst[onuId].pop();

    SimTime packetTime(
        static_cast<double>(packet->getBitLength()) / dataRate);

    EV << "OLT transmitting packet for ONU " << onuId
       << " at " << simTime()
       << ", packet transmission time = " << packetTime
       << "\n";
    packet->setKind(3);// Set kind to 3 for downstream packets.
    send(packet, "out");

    // The next event occurs after this packet's transmission time.
    scheduleAt(simTime() + packetTime, downTransmitMsg);
}

void OLT::finishCurrentBurst()
{
    int onuId = activeONU;

    if (onuId < 0)
        return;

    EV << "Completed downstream burst for ONU "
       << onuId << " at " << simTime() << "\n";

    // Clear the active transmission before scheduling another ONU.
    activeONU = -1;

    burstReserved[onuId] = false;

    // New burst is appended at horizonDown, after all existing
    // reservations and their guard intervals.
    reserveNextBurst(onuId);
}


void OLT::handleMessage(cMessage *msg)
{

    // ----------------------------------------
    // DOWNSTREAM: packet generation
    // ----------------------------------------
    for (int i = 0; i < numONUs; ++i) {
        if (msg == generateMsg[i]) {
            cPacket *packet = new cPacket("EPON Packet");
            packet->setBitLength(downstreamPacketSizeBits);
            packet->addPar("destinationONU");
            packet->par("destinationONU").setLongValue(i);
            nextBurst[i].push(packet);

            EV_INFO << "OLT generated downstream packet for ONU "
                    << i << " at " << simTime() << endl;

            scheduleAt(
                simTime() + exponential(downstreamPacketInterval),
                generateMsg[i]);

            if (downstreamSchedulingStarted && !burstReserved[i]) {
                if (horizonDown < simTime())
                    horizonDown = simTime();
                reserveNextBurst(i);
            }
            return;
        }
    }

    // ----------------------------------------
    // DOWNSTREAM: initial reservation event
    // ----------------------------------------
    if (msg == downScheduleMsg) {
        downstreamSchedulingStarted = true;
        EV_INFO << "\n=== INITIAL DOWNSTREAM SCHEDULING ==="
                << "\nTime = " << simTime() << endl;

        for (int i = 0; i < numONUs; ++i)
            reserveNextBurst(i);

        EV_INFO << "Initial downstream reservations complete."
                << "\nFinal horizonDown = " << horizonDown << endl;
        return;
    }

    // ----------------------------------------
    // DOWNSTREAM: start reserved burst
    // ----------------------------------------
    for (int i = 0; i < numONUs; ++i) {
        if (msg == burstStartMsg[i]) {
            startBurst(i);
            return;
        }
    }

    // ----------------------------------------
    // DOWNSTREAM: transmit next packet
    // ----------------------------------------
    if (msg == downTransmitMsg) {
        transmitNextPacket();
        return;
    }

    // ----------------------------------------
    // Initial discovery event
    // ----------------------------------------
    
    if (msg == scheduleMsg)
    {
        // ========================================
        // PHASE 1: INITIAL DISCOVERY
        // ========================================
        if (!initialScheduleDone)
        {
            initialScheduleDone = true;
            phase = SchedulerPhase::INITIAL_DISCOVERY;

            EV_INFO << "\n=== INITIAL DISCOVERY ==="
                    << "\nSending zero-byte GATEs to all ONUs"
                    << endl;

            for (int i = 0; i < numONUs; i++)
            {
                sendGrantForONU(i, 0);
            }

            return;
        }

        // ========================================
        // PHASE 2: INITIAL LOCAL SHARING
        // ========================================
        if (phase == SchedulerPhase::INITIAL_LOCAL_SHARING)
        {
            EV_INFO << "\n=== INITIAL LOCAL SHARING ==="
                    << endl;

            // Calculate the initial schedule, send the grants,
            // and establish horizonLS and horizonUP.
            scheduleInitialLocalSharing();

            // Initialization is complete once the schedule has
            // been calculated and the reservations established.
            phase = SchedulerPhase::STEADY_STATE;

            SimTime nextScheduleTime =
                horizonLS - SimTime(125, SIMTIME_US);

            if (nextScheduleTime <= simTime())
            {
                throw cRuntimeError(
                    "Next LS scheduling deadline is not in the future. "
                    "Check the initial LS reservation and 125us lead time."
                );
            }

            EV_INFO << "\nInitial scheduling complete"
                    << "\nNext LS scheduling time = "
                    << nextScheduleTime
                    << "\nHorizon LS = " << horizonLS
                    << "\nHorizon UP = " << horizonUP
                    << endl;

            scheduleAt(nextScheduleTime, scheduleMsg);

            return;
        }

        // ========================================
        // PHASE 3: DYNAMIC LOCAL SHARING
        // ========================================
        if (phase == SchedulerPhase::STEADY_STATE)
        {
            EV_INFO << "\n=== DYNAMIC LOCAL SHARING ==="
                    << "\nScheduling time = " << simTime()
                    << "\nHorizon LS = " << horizonLS
                    << "\nHorizon UP = " << horizonUP
                    << endl;

            const bool lsCommitted = scheduleDynamicLocalSharing();
            if (!lsCommitted)
                return; // A future REPORT will trigger another decision.

            SimTime nextScheduleTime =
                horizonLS - SimTime(125, SIMTIME_US);

            if (nextScheduleTime <= simTime())
            {
                throw cRuntimeError(
                    "Next dynamic LS scheduling deadline is not "
                    "in the future"
                );
            }

            if (!scheduleMsg->isScheduled())
                scheduleAt(nextScheduleTime, scheduleMsg);

            return;
        }

        return;
    }



    // ----------------------------------------
    // REPORT reception
    // ----------------------------------------
    if (msg->isName("REPORT"))
    {
        if (!msg->hasPar("onuId") ||
            !msg->hasPar("queueBytes"))
        {
            delete msg;
            throw cRuntimeError(
                "REPORT missing onuId or queueBytes"
            );
        }

        int onuId =
            static_cast<int>(msg->par("onuId").longValue());

        long reportedBytes =
            msg->par("queueBytes").longValue();

        if (onuId < 0 || onuId >= numONUs || reportedBytes < 0)
        {
            delete msg;
            throw cRuntimeError("Invalid REPORT contents");
        }

        queueBytes[onuId] = reportedBytes;
        isScheduled[onuId] = false;

        EV_INFO << "\nOLT received REPORT"
                << "\nReported source ONU = " << onuId
                << "\nQueue bytes = " << reportedBytes
                << "\nPhase = " << static_cast<int>(phase)
                << endl;

        if (phase == SchedulerPhase::INITIAL_DISCOVERY)
        {
            if (!initialReportReceived[onuId])
            {
                initialReportReceived[onuId] = true;
                initialReportsRemaining--;

                EV_INFO << "Initial REPORT count remaining = "
                        << initialReportsRemaining
                        << endl;
            }

            delete msg;

            if (initialReportsRemaining == 0)
                selectInitialLocalSharingGroup();

            return;
        }

        if (phase == SchedulerPhase::INITIAL_LOCAL_SHARING)
        {
            // This phase is only for calculating and issuing the initial
            // Local Sharing schedule. It should not wait for the Host REPORT.
            delete msg;
            return;
        }

        // In steady state, a new REPORT re-enables this ONU. If a prior
        // LS opportunity was deferred and no JIT event is pending, retry now
        // (or at the next JIT deadline if it is still in the future).
        delete msg;
        if (phase == SchedulerPhase::STEADY_STATE && !scheduleMsg->isScheduled())
        {
            SimTime nextDecision = horizonLS - SimTime(125, SIMTIME_US);
            if (nextDecision < simTime())
            {    
                nextDecision = simTime();
                horizonLS = nextDecision + SimTime(125, SIMTIME_US);
            }
            scheduleAt(nextDecision, scheduleMsg);
        }
        return;
    }

    delete msg;
}


void OLT::finish()
{

    // Clean up downstream queues and scheduled messages.
    for (int i = 0; i < numONUs; ++i) {
        while (!currentBurst[i].empty()) {
            delete currentBurst[i].front();
            currentBurst[i].pop();
        }
        while (!nextBurst[i].empty()) {
            delete nextBurst[i].front();
            nextBurst[i].pop();
        }
        cancelAndDelete(generateMsg[i]);
        cancelAndDelete(burstStartMsg[i]);
    }

    cancelAndDelete(downScheduleMsg);
    cancelAndDelete(downTransmitMsg);

    cancelAndDelete(scheduleMsg);
}
