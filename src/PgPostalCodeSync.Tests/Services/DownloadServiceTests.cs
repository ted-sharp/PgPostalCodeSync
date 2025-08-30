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
        _mockLogger = new Mock<ILogger<DownloadService>>();
        _httpClient = new HttpClient();
        _downloadService = new DownloadService(_mockLogger.Object, _httpClient);
        _tempDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(_tempDirectory);
    }

    public void Dispose()
    {
        _httpClient?.Dispose();
        if (Directory.Exists(_tempDirectory))
        {
            Directory.Delete(_tempDirectory, true);
        }
    }

    [Fact]
    public async Task ValidateFileAsync_ExistingFile_ShouldReturnTrue()
    {
        // Arrange
        var testFile = Path.Combine(_tempDirectory, "test.txt");
        await File.WriteAllTextAsync(testFile, "test content");

        // Act
        var result = await _downloadService.ValidateFileAsync(testFile);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task ValidateFileAsync_NonExistentFile_ShouldReturnFalse()
    {
        // Arrange
        var nonExistentFile = Path.Combine(_tempDirectory, "nonexistent.txt");

        // Act
        var result = await _downloadService.ValidateFileAsync(nonExistentFile);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ValidateFileAsync_FileSmallerThanMinimumSize_ShouldReturnFalse()
    {
        // Arrange
        var testFile = Path.Combine(_tempDirectory, "small.txt");
        await File.WriteAllTextAsync(testFile, "small");
        var minimumSize = 1000L;

        // Act
        var result = await _downloadService.ValidateFileAsync(testFile, minimumSize);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ValidateFileAsync_FileLargerThanMinimumSize_ShouldReturnTrue()
    {
        // Arrange
        var testFile = Path.Combine(_tempDirectory, "large.txt");
        var content = new string('A', 2000);
        await File.WriteAllTextAsync(testFile, content);
        var minimumSize = 1000L;

        // Act
        var result = await _downloadService.ValidateFileAsync(testFile, minimumSize);

        // Assert
        Assert.True(result);
    }
}