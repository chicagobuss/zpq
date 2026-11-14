# Windows installation

Download `zpq-x86_64-windows-gnu.zip` and its `.sha256` file from the release.
In PowerShell, compare `Get-FileHash .\zpq-x86_64-windows-gnu.zip -Algorithm SHA256`
with the digest in the checksum file. Extract the archive, then run `bin\zpq.exe --version`.
Add the extracted `bin` directory to your user PATH if you want to run `zpq` from other directories.

For example, from the extracted directory:

```powershell
.\bin\zpq.exe query .\data.parquet --aggregate 'count(*)'
.\bin\zpq.exe query .\data.parquet --output .\copy.parquet
```

The x86-64 Windows build uses Windows certificate chain policy for HTTPS. UPDATE is not yet available on Windows.
