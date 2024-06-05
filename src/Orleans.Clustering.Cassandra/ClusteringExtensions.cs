using System;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

using Orleans.Clustering.Cassandra.Membership;
using Orleans.Clustering.Cassandra.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Messaging;

namespace Orleans.Clustering.Cassandra
{
    public static class ClusteringExtensions
    {
        public static ISiloBuilder UseCassandraClustering(
            this ISiloBuilder builder, 
            Func</*IConfiguration,*/ IConfiguration> configurationProvider)
        {
            return builder.ConfigureServices(
                (context/*,services*/) => context.UseCassandraClustering(ob => ob.Bind(configurationProvider(/*context.Configuration*/))));
        }

        public static ISiloBuilder UseCassandraClustering(this ISiloBuilder builder, Action<CassandraClusteringOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseCassandraClustering(ob => ob.Configure(configureOptions)));
        }

        public static ISiloBuilder UseCassandraClustering(
            this ISiloBuilder builder,
            Action<OptionsBuilder<CassandraClusteringOptions>> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseCassandraClustering(configureOptions));
        }

        public static IServiceCollection UseCassandraClustering(
            this IServiceCollection services,
            Action<OptionsBuilder<CassandraClusteringOptions>> configureOptions)
        {
            configureOptions?.Invoke(services.AddOptions<CassandraClusteringOptions>());
            return services.AddSingleton<IMembershipTable, CassandraMembershipTable>();
        }

        public static IClientBuilder UseCassandraGatewayListProvider(
            this IClientBuilder builder, 
            Func</*IConfiguration,*/ IConfiguration> configurationProvider)
        {
            return builder.ConfigureServices(
                (context/*, services*/) => context.UseCassandraGatewayListProvider(ob => ob.Bind(configurationProvider(/*context.Configuration*/))));
        }

        public static IClientBuilder UseCassandraGatewayListProvider(this IClientBuilder builder, Action<CassandraClusteringOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseCassandraGatewayListProvider(ob => ob.Configure(configureOptions)));
        }

        public static IClientBuilder UseCassandraGatewayListProvider(
            this IClientBuilder builder,
            Action<OptionsBuilder<CassandraClusteringOptions>> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseCassandraGatewayListProvider(configureOptions));
        }

        public static IServiceCollection UseCassandraGatewayListProvider(
            this IServiceCollection services,
            Action<OptionsBuilder<CassandraClusteringOptions>> configureOptions)
        {
            configureOptions?.Invoke(services.AddOptions<CassandraClusteringOptions>());
            return services.AddSingleton<IGatewayListProvider, CassandraGatewayListProvider>();
        }
    }
}