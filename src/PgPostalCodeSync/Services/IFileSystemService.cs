namespace PgPostalCodeSync.Services;

public interface IFileSystemService
{
    void CreateDirectory(string path);
    bool DirectoryExists(string path);
    bool FileExists(string path);
    void DeleteDirectory(string path, bool recursive = false);
    void DeleteFile(string path);
}

public class FileSystemService : IFileSystemService
{
    public void CreateDirectory(string path)
    {
        Directory.CreateDirectory(path);
    }

    public bool DirectoryExists(string path)
    {
        return Directory.Exists(path);
    }

    public bool FileExists(string path)
    {
        return File.Exists(path);
    }

    public void DeleteDirectory(string path, bool recursive = false)
    {
        Directory.Delete(path, recursive);
    }

    public void DeleteFile(string path)
    {
        File.Delete(path);
    }
}