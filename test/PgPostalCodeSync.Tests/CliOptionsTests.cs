using Xunit;
using PgPostalCodeSync;

namespace PgPostalCodeSync.Tests;

public class CliOptionsTests
{
    [Fact]
    public void ResolveYyMm_WithExplicitYyMm_ReturnsExplicitValue()
    {
        // Arrange
        var options = new CliOptions { YyMm = "2508" };
        
        // Act
        var result = options.ResolveYyMm();
        
        // Assert
        Assert.Equal("2508", result);
    }
    
    [Fact]
    public void ResolveYyMm_WithoutYyMm_ReturnsPreviousMonth()
    {
        // Arrange
        var options = new CliOptions();
        var testDateTime = new DateTime(2025, 9, 15); // September 2025
        
        // Act
        var result = options.ResolveYyMm(testDateTime);
        
        // Assert
        Assert.Equal("2508", result); // Previous month: August 2025 -> 2508
    }
    
    [Theory]
    [InlineData("2508", true)]
    [InlineData("0101", true)]
    [InlineData("9912", true)]
    [InlineData("1", false)]
    [InlineData("25080", false)]
    [InlineData("2513", false)] // Invalid month
    [InlineData("abc", false)]
    public void ResolveYyMm_ValidatesFormat(string yymm, bool isValid)
    {
        // Arrange
        var options = new CliOptions { YyMm = yymm };
        
        // Act & Assert
        if (isValid)
        {
            var result = options.ResolveYyMm();
            Assert.Equal(yymm, result);
        }
        else
        {
            Assert.Throws<ArgumentException>(() => options.ResolveYyMm());
        }
    }
    
    [Fact]
    public void ResolveVersionDate_ReturnsFirstDayOfMonth()
    {
        // Arrange
        var options = new CliOptions { YyMm = "2508" };
        
        // Act
        var result = options.ResolveVersionDate();
        
        // Assert
        Assert.Equal(new DateTime(2025, 8, 1), result);
    }
}