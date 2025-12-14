using Microsoft.Extensions.Options;

namespace Shuttle.Hopper.SqlServer.Queue;

public class SqlServerQueueOptionsValidator : IValidateOptions<SqlServerQueueOptions>
{
    public ValidateOptionsResult Validate(string? name, SqlServerQueueOptions options)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return ValidateOptionsResult.Fail(Resources.TransportConfigurationNameException);
        }

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return ValidateOptionsResult.Fail(string.Format(Resources.TransportConfigurationItemException, name, nameof(options.ConnectionString)));
        }

        if (string.IsNullOrWhiteSpace(options.Schema))
        {
            return ValidateOptionsResult.Fail(string.Format(Resources.TransportConfigurationItemException, name, nameof(options.Schema)));
        }

        return ValidateOptionsResult.Success;
    }
}