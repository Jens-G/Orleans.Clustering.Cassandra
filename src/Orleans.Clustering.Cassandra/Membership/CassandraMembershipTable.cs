using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Orleans.Clustering.Cassandra.Membership.Models;
using Orleans.Clustering.Cassandra.Options;
using Orleans.Configuration;
using Orleans.Runtime;

namespace Orleans.Clustering.Cassandra.Membership
{
    /*
    enum SiloStatus
        None            0   No known status.
        Created         1   This silo was just created, but not started yet.
        Joining         2   This silo has just started, but not ready yet. It is attempting to join the cluster.
        Active          3   This silo is alive and functional.
        ShuttingDown    4   This silo is shutting itself down.
        Stopping        5   This silo is stopping itself down.
        Dead            6   This silo is deactivated/considered to be dead.
    */


    internal class CassandraMembershipTable : IMembershipTable
    {
        private const ConsistencyLevel DefaultConsistencyLevel = ConsistencyLevel.EachQuorum;

        private readonly string _clusterId;
        private readonly CassandraClusteringOptions _cassandraClusteringOptions;
        private readonly ILogger<CassandraMembershipTable> _logger;

        private Mapper _mapper;
        private Table<ClusterMembership> _dataTable;

        public CassandraMembershipTable(
            IOptions<ClusterOptions> clusterOptions,
            IOptions<CassandraClusteringOptions> cassandraClusteringOptions,
            ILogger<CassandraMembershipTable> logger,
            ILoggerProvider loggerProvider)
        {
            _clusterId = clusterOptions.Value.ClusterId;
            _cassandraClusteringOptions = cassandraClusteringOptions.Value;
            _logger = logger;

            Diagnostics.CassandraPerformanceCountersEnabled = _cassandraClusteringOptions.Diagnostics.PerformanceCountersEnabled;
            Diagnostics.CassandraStackTraceIncluded = _cassandraClusteringOptions.Diagnostics.StackTraceIncluded;

            if (loggerProvider != null)
            {
                Diagnostics.AddLoggerProvider(loggerProvider);
            }
        }

        public async Task InitializeMembershipTable(bool tryInitTableVersion)
        {
            try
            {
                var cassandraCluster =
                    Cluster.Builder()
                           .AddContactPoints(_cassandraClusteringOptions.ContactPoints.Split(','))
                           .Build();

                var session = await cassandraCluster.ConnectAsync();
                await Task.Run(
                    () =>
                        {
                            var keyspace = _cassandraClusteringOptions.Keyspace;
                            session.CreateKeyspaceIfNotExists(
                                keyspace,
                                new Dictionary<string, string>
                                    {
                                        { "class", "SimpleStrategy" },
                                        { "replication_factor", _cassandraClusteringOptions.ReplicationFactor.ToString() }
                                    });

                            session.ChangeKeyspace(keyspace);
                        });

                var mappingConfiguration = new MappingConfiguration().Define(new EntityMappings(_cassandraClusteringOptions.TableName));

                _dataTable = new Table<ClusterMembership>(session, mappingConfiguration);
                await Task.Run(() => _dataTable.CreateIfNotExists());

                _mapper = new Mapper(session, mappingConfiguration);

                if (tryInitTableVersion)
                {
                    await _mapper.InsertAsync(
                        ClusterVersion.New(_clusterId),
                        CqlQueryOptions.New().SetConsistencyLevel(DefaultConsistencyLevel));
                }
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while initializing membership data table for cluster {clusterId}.", _clusterId);
                throw;
            }
        }

        public async Task DeleteMembershipTableEntries(string clusterId)
        {
            try
            {
                var data = await _dataTable
                                 .Where(x => x.ClusterId == _clusterId)
                                 .AllowFiltering()
                                 .SetConsistencyLevel(DefaultConsistencyLevel)
                                 .ExecuteAsync();

                var batch = _mapper.CreateBatch().WithOptions(x => x.SetConsistencyLevel(DefaultConsistencyLevel));
                foreach (var item in data)
                {
                    batch.Delete(item);
                }

                await _mapper.ExecuteAsync(batch);
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while deleting membership data for cluster {clusterId}.", clusterId);
                throw;
            }
        }

        public async Task<MembershipTableData> ReadRow(SiloAddress key)
        {
            try
            {
                var entityId = key.AsSiloInstanceId();
                var ids = new[] { entityId, ClusterVersion.Id };
                var data = await _dataTable
                                 .Where(x => x.ClusterId == _clusterId && ids.Contains(x.EntityId))
                                 .AllowFiltering()
                                 .SetConsistencyLevel(DefaultConsistencyLevel)
                                 .ExecuteAsync();

                return CreateMembershipTableData(data);
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while reading data for silo with key {siloKey}.", key.ToString());
                throw;
            }
        }

        public async Task<MembershipTableData> ReadAll()
        {
            try
            {
                var data = await _dataTable
                                 .Where(x => x.ClusterId == _clusterId)
                                 .AllowFiltering()
                                 .SetConsistencyLevel(DefaultConsistencyLevel)
                                 .ExecuteAsync();

                return CreateMembershipTableData(data);
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while reading all cluster membership data.");
                throw;
            }
        }

        public async Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
        {
            try
            {
                var siloInstance = entry.AsSiloInstance(_clusterId);
                var clusterVersion = tableVersion.AsClusterVersion(_clusterId);

                var batch = _mapper.CreateBatch().WithOptions(x => x.SetConsistencyLevel(DefaultConsistencyLevel));
                batch.Insert(siloInstance);
                batch.Update(clusterVersion);

                await _mapper.ExecuteAsync(batch);

                return true;
            }
            catch (DriverException)
            {
                _logger.LogWarning(
                    "Cassandra driver error occured while inserting row for silo {silo}, cluster version = {clusterVersion}.",
                    entry.ToString(),
                    tableVersion.Version);
                throw;
            }
        }

        public async Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            try
            {
                var siloInstance = entry.AsSiloInstance(_clusterId);
                var clusterVersion = tableVersion.AsClusterVersion(_clusterId);

                var batch = _mapper.CreateBatch().WithOptions(x => x.SetConsistencyLevel(DefaultConsistencyLevel));
                batch.Update(siloInstance);
                batch.Update(clusterVersion);

                await _mapper.ExecuteAsync(batch);

                return true;
            }
            catch (DriverException)
            {
                _logger.LogWarning(
                    "Cassandra driver error occured while updating row for silo {silo}, cluster version = {clusterVersion}.",
                    entry.ToString(),
                    tableVersion.Version);
                throw;
            }
        }

        public async Task UpdateIAmAlive(MembershipEntry entry)
        {
            try
            {
                var entityId = entry.SiloAddress.AsSiloInstanceId();
                await _mapper.UpdateAsync<SiloInstance>(
                    Cql.New(
                           $"SET {nameof(SiloInstance.IAmAliveTime)} = ? " +
                           $"WHERE {nameof(SiloInstance.ClusterId)} = ? AND {nameof(SiloInstance.EntityId)} = ? ",
                           entry.IAmAliveTime,
                           _clusterId,
                           entityId)
                       .WithOptions(x => x.SetConsistencyLevel(DefaultConsistencyLevel)));
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while updating liveness status for silo {silo}.", entry.ToString());
                throw;
            }
        }

        private MembershipTableData CreateMembershipTableData(IEnumerable<ClusterMembership> data)
        {
            TableVersion tableVersion = null;
            var members = new List<Tuple<MembershipEntry, string>>();
            foreach (var item in data)
            {
                if (item.EntityId == ClusterVersion.Id)
                {
                    tableVersion = item.AsTableVersion();
                }
                else if (!string.IsNullOrEmpty(item.Address))
                {
                    var entry = item.AsMembershipEntry();
                    members.Add(Tuple.Create(entry, string.Empty));
                }
                else 
                {
                    if (_logger.IsEnabled(LogLevel.Information))
                        _logger.LogInformation("IMembershipTable.CreateMembershipTableData skipped broken record {item}.", item.EntityId);
                }
            }

            return new MembershipTableData(members, tableVersion);
        }

        public async Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
                _logger.LogTrace("IMembershipTable.CleanupDefunctSiloEntries called with beforeDate {beforeDate} and clusterId {ClusterId}.", beforeDate, _clusterId);
            try
            {
                // Caveat: We have to provide the entire PK at DELETE, otherwise an error about missing PK parts is raised
                // PS: no filtering allowed at DELETE either :-(

                var entities = await _mapper.FetchAsync<SiloInstance>(
                    Cql.New(
                       $"WHERE {nameof(SiloInstance.ClusterId)} = ? AND {nameof(SiloInstance.IAmAliveTime)} < ? ALLOW FILTERING",
                       _clusterId,
                       beforeDate)
                   .WithOptions(x => x.SetConsistencyLevel(DefaultConsistencyLevel)));

                // remove any active or non-node entries, keep outdated inactive nodes and broken data
                entities = entities.Where( (row) => string.IsNullOrEmpty(row.Address)             // broken data
                                                    || (row.Status != (int)SiloStatus.Active)     // outdated and not active
                                                    || (!row.EntityId.Equals(ClusterVersion.Id))  // is real node entry
                                                    );
                await DeleteEntities(entities);

            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "CassandraMembershipTable.CleanupDefunctSiloEntries failed");
                throw;
            }
        }

        private async Task DeleteEntities(IEnumerable<SiloInstance> entities)
        {
            foreach (var entity in entities)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogTrace("CassandraMembershipTable.DeleteEntities: deleting {ClusterId} / {Entityid}", _clusterId, entity.EntityId);

                await _mapper.DeleteAsync<SiloInstance>(
                    Cql.New(
                           $"WHERE {nameof(SiloInstance.ClusterId)} = ? AND {nameof(SiloInstance.EntityId)} = ?",
                           _clusterId,
                           entity.EntityId)
                       .WithOptions(x => x.SetConsistencyLevel(DefaultConsistencyLevel)));
            }
        }
    }
}