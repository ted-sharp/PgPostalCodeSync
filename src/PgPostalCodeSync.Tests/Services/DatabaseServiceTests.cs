using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using PgPostalCodeSync.Models;
using PgPostalCodeSync.Services;

namespace PgPostalCodeSync.Tests.Services;

public class DatabaseServiceTests
{
    private readonly Mock<ILogger<DatabaseService>> _mockLogger;
    private readonly Mock<IConfiguration> _mockConfiguration;
    private readonly DatabaseService _databaseService;

    public DatabaseServiceTests()
    {
        this._mockLogger = new Mock<ILogger<DatabaseService>>();
        this._mockConfiguration = new Mock<IConfiguration>();

        // Mock the ConnectionStrings section
        var mockConnectionStringsSection = new Mock<IConfigurationSection>();
        var mockPostalDbConnectionSection = new Mock<IConfigurationSection>();
        mockPostalDbConnectionSection.Setup(x => x.Value).Returns("Host=localhost;Database=testdb;Username=test;Password=test");

        mockConnectionStringsSection.Setup(x => x["PostalDb"]).Returns("Host=localhost;Database=testdb;Username=test;Password=test");
        mockConnectionStringsSection.Setup(x => x.GetSection("PostalDb")).Returns(mockPostalDbConnectionSection.Object);

        this._mockConfiguration.Setup(x => x.GetSection("ConnectionStrings")).Returns(mockConnectionStringsSection.Object);

        this._databaseService = new DatabaseService(this._mockLogger.Object, this._mockConfiguration.Object);
    }

    [Fact]
    public async Task HasExistingDataAsync_WithInvalidConnection_ShouldReturnFalse()
    {
        // Act
        var result = await this._databaseService.HasExistingDataAsync();

        // Assert - Should return false due to connection failure
        Assert.False(result);
    }

    [Fact]
    public async Task CreateIngestionRunAsync_ShouldThrowWithInvalidConnection()
    {
        // Arrange
        var runType = "FULL";
        var status = "RUNNING";

        // Act & Assert - Should throw due to connection failure
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await this._databaseService.CreateIngestionRunAsync(runType, status));
    }

    [Fact]
    public async Task BulkInsertPostalCodesAsync_ShouldThrowWithInvalidConnection()
    {
        // Arrange
        var records = new[]
        {
            new PostalCodeRecord
            {
                LocalGovernmentCode = "01101",
                ZipCode7 = "0600000",
                Prefecture = "北海道",
                City = "札幌市中央区",
                Town = ""
            }
        };

        // Act & Assert - Should throw due to connection failure
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await this._databaseService.BulkInsertPostalCodesAsync(records));
    }

    [Fact]
    public async Task RenameTableAsync_ShouldThrowWithInvalidConnection()
    {
        // Arrange
        var fromTableName = "postal_codes_new";
        var toTableName = "postal_codes";

        // Act & Assert - Should throw due to connection failure
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await this._databaseService.RenameTableAsync(fromTableName, toTableName));
    }

    [Fact]
    public async Task BackupTableAsync_ShouldThrowWithInvalidConnection()
    {
        // Arrange
        var tableName = "postal_codes";
        var backupSuffix = "20250830_123456";

        // Act & Assert - Should throw due to connection failure
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await this._databaseService.BackupTableAsync(tableName, backupSuffix));
    }
}
