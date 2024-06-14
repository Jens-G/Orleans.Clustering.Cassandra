using System;
using System.Collections.Generic;
using System.Net;

using Orleans.Runtime;

namespace Orleans.Clustering.Cassandra.Membership.Models
{
    internal static class EntityExtensions
    {
        public static TableVersion AsTableVersion(this IClusterVersion clusterVersion)
            => new(clusterVersion.Version, string.Empty);

        public static ClusterVersion AsClusterVersion(this TableVersion tableVersion, string clusterId)
            => new()
            {
                ClusterId = clusterId,
                Timestamp = DateTimeOffset.UtcNow,
                Version = tableVersion.Version
            };

        public static MembershipEntry AsMembershipEntry(this ISiloInstance siloInstance)
        {
            var entry = new MembershipEntry
                {
                    SiloName = siloInstance.SiloName,
                    HostName = siloInstance.HostName,
                    Status = (SiloStatus)siloInstance.Status,
                    RoleName = siloInstance.RoleName,
                    UpdateZone = siloInstance.UpdateZone,
                    FaultZone = siloInstance.FaultZone,
                    StartTime = siloInstance.StartTime.UtcDateTime,
                    IAmAliveTime = siloInstance.IAmAliveTime.UtcDateTime,
                    SiloAddress = SiloAddress.New(new IPEndPoint(IPAddress.Parse(siloInstance.Address), siloInstance.Port), siloInstance.Generation)
                };

            if (siloInstance.ProxyPort.HasValue)
            {
                entry.ProxyPort = siloInstance.ProxyPort.Value;
            }

            var suspectingSilos = new List<SiloAddress>();
            var suspectingTimes = new List<DateTime>();

            foreach (var silo in siloInstance.SuspectingSilos)
            {
                suspectingSilos.Add(SiloAddress.FromParsableString(silo));
            }

            foreach (var time in siloInstance.SuspectingTimes)
            {
                suspectingTimes.Add(time.UtcDateTime);
            }

            if (suspectingSilos.Count != suspectingTimes.Count)
            {
                throw new OrleansException(
                    $"SuspectingSilos.Length of {suspectingSilos.Count} as read from Cassandra " +
                    $"is not equal to SuspectingTimes.Length of {suspectingTimes.Count}");
            }

            for (var i = 0; i < suspectingSilos.Count; i++)
            {
                entry.AddSuspector(suspectingSilos[i], suspectingTimes[i]);
            }

            return entry;
        }

        public static SiloInstance AsSiloInstance(this MembershipEntry entry, string clusterId)
        {
            var siloInstance = new SiloInstance
                {
                    ClusterId = clusterId,
                    EntityId = entry.SiloAddress.AsSiloInstanceId(),
                    Address = entry.SiloAddress.Endpoint.Address.ToString(),
                    Port = entry.SiloAddress.Endpoint.Port,
                    Generation = entry.SiloAddress.Generation,
                    SiloName = entry.SiloName,
                    HostName = entry.HostName,
                    Status = (int)entry.Status,
                    ProxyPort = entry.ProxyPort,
                    RoleName = entry.RoleName,
                    UpdateZone = entry.UpdateZone,
                    FaultZone = entry.FaultZone,
                    StartTime = entry.StartTime.ToUniversalTime(),
                    IAmAliveTime = entry.IAmAliveTime.ToUniversalTime()
                };

            if (entry.SuspectTimes != null)
            {
                siloInstance.SuspectingSilos ??= [];
                siloInstance.SuspectingTimes ??= [];

                foreach (var tuple in entry.SuspectTimes)
                {
                    siloInstance.SuspectingSilos.Add(tuple.Item1.ToParsableString());
                    siloInstance.SuspectingTimes.Add(tuple.Item2);
                }
            }

            return siloInstance;
        }
    }
}