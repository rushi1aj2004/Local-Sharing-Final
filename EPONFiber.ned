channel EPONFiber extends ned.DelayChannel
{
    parameters:
        double propagationDelay @unit(s) @mutable= default(100us);
        delay @unit(s) @mutable= propagationDelay;
}
