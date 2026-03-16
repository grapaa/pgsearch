$dirs = Get-ChildItem "C:\git\byggesakindexer\data\raw" -Directory | Where-Object { $_.Name -like "2025-12-*" }

foreach ($dir in $dirs) {
    robocopy $dir.FullName "C:\git\pgsearch\SampleData\$($dir.Name)" /S /XF *.json /NFL /NDL /NJH /NJS /NC /NS /NP
}

Write-Host "Done!" -ForegroundColor Green
