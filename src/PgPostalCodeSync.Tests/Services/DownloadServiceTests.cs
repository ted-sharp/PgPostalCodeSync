using Microsoft.Extensions.Logging;
using Moq;
using PgPostalCodeSync.Services;
using System.Text;

namespace PgPostalCodeSync.Tests.Services;

public class DownloadServiceTests : IDisposable
{
    private readonly Mock<ILogger<DownloadService>> _mockLogger;
    private readonly HttpClient _httpClient;
    private readonly DownloadService _downloadService;
    private readonly string _tempDirectory;

    public DownloadServiceTests()
    {
        this._mockLogger = new Mock<ILogger<DownloadService>>();
        this._httpClient = new HttpClient();
        this._downloadService = new DownloadService(this._mockLogger.Object, this._httpClient);
        this._tempDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(this._tempDirectory);
    }

    public void Dispose()
    {
        this._httpClient?.Dispose();
        if (Directory.Exists(this._tempDirectory))
        {
            Directory.Delete(this._tempDirectory, true);
        }
    }

    [Fact]
    public async Task ValidateFileAsync_ExistingFile_ShouldReturnTrue()
    {
        // Arrange
        var testFile = Path.Combine(this._tempDirectory, "test.txt");
        await File.WriteAllTextAsync(testFile, "test content");

        // Act
        var result = await this._downloadService.ValidateFileAsync(testFile);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task ValidateFileAsync_NonExistentFile_ShouldReturnFalse()
    {
        // Arrange
        var nonExistentFile = Path.Combine(this._tempDirectory, "nonexistent.txt");

        // Act
        var result = await this._downloadService.ValidateFileAsync(nonExistentFile);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ValidateFileAsync_FileSmallerThanMinimumSize_ShouldReturnFalse()
    {
        // Arrange
        var testFile = Path.Combine(this._tempDirectory, "small.txt");
        await File.WriteAllTextAsync(testFile, "small");
        var minimumSize = 1000L;

        // Act
        var result = await this._downloadService.ValidateFileAsync(testFile, minimumSize);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ValidateFileAsync_FileLargerThanMinimumSize_ShouldReturnTrue()
    {
        // Arrange
        var testFile = Path.Combine(this._tempDirectory, "large.txt");
        var content = new string('A', 2000);
        await File.WriteAllTextAsync(testFile, content);
        var minimumSize = 1000L;

        // Act
        var result = await this._downloadService.ValidateFileAsync(testFile, minimumSize);

        // Assert
        Assert.True(result);
    }
}
