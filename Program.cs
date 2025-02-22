using Serilog;

namespace KafkaMicroservice
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = Host.CreateApplicationBuilder(args);
            //Clear Providers 
            builder.Logging.ClearProviders();
            //Read appsettings.json
            Log.Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(builder.Configuration)
                .CreateLogger();
            // add the provider
            builder.Logging.AddSerilog();
            builder.Services.AddHostedService<Worker>();

            var host = builder.Build();
            host.Run();
        }
    }
}