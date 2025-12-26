using Microsoft.Extensions.Options;

namespace Shuttle.Hopper.SqlServer.Queue;

public class SqlServerQueueOptionsValidator : IValidateOptions<SqlServerQueueOptions>
{
    public ValidateOptionsResult Validate(string? name, SqlServerQueueOptions options)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return ValidateOptionsResult.Fail(Hopper.Resources.TransportConfigurationNameException);
        }

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            return ValidateOptionsResult.Fail(string.Format(Hopper.Resources.TransportConfigurationItemException, name, nameof(options.ConnectionString)));
        }

        if (string.IsNullOrWhiteSpace(options.Schema))
        {
            return ValidateOptionsResult.Fail(string.Format(Hopper.Resources.TransportConfigurationItemException, name, nameof(options.Schema)));
        }

        if (!System.Text.RegularExpressions.Regex.IsMatch(options.Schema, "^[a-zA-Z_][a-zA-Z0-9_]*$"))
        {
            return ValidateOptionsResult.Fail(Resources.SchemaIdentifierException);
        }

        return ValidateOptionsResult.Success;
    }
}