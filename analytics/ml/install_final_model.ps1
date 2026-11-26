[CmdletBinding(DefaultParameterSetName = 'Local')]
param(
    [Parameter(ParameterSetName = 'Local')]
    [string] $ArchivePath,

    [Parameter(Mandatory, ParameterSetName = 'Release')]
    [switch] $FromRelease,

    [ValidatePattern('^/trendcast/analytics/wikimedia_ml_checks/install-[a-z0-9-]+$')]
    [string] $VerificationRoot
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$version = 'hala-j1-20261126-v1'
$release = 'hala-ml-j1-20261126-r2'
$repository = 'Urgsalman/trendcast-big-data-platform'
$archiveName = 'TrendCast_Hala_ML_Final_hala-j1-20261126-r2.zip'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$target = if ($VerificationRoot) { $VerificationRoot } else {
    "/trendcast/models/wikimedia_next_day_views/candidates/$version"
}
$id = [guid]::NewGuid().ToString('N')
$temp = Join-Path ([IO.Path]::GetTempPath()) "tcml-$($id.Substring(0, 8))"
$nodeTemp = "/tmp/trendcast-ml-$id"

function Invoke-Checked {
    param([string] $Program, [string[]] $CommandArgs)

    $output = & $Program @CommandArgs 2>&1
    $code = $LASTEXITCODE
    if ($code -ne 0) {
        throw "$Program a échoué (code $code). $($output -join [Environment]::NewLine)"
    }
    return $output
}

function Invoke-NameNode {
    param([string[]] $CommandArgs)

    Invoke-Checked -Program 'docker' -CommandArgs (@('compose', 'exec', '-T', 'namenode') + $CommandArgs)
}

function Test-ImportedFiles {
    param([string] $HdfsPath, [string] $Label)

    $check = @'
set -euo pipefail
hdfs dfs -get "$1" "$2"
cd "$2"
sha256sum --check --strict "$3"
find . -type f -printf '%P\n' | LC_ALL=C sort > "$2.files"
cut -c 67- "$3" | LC_ALL=C sort > "$2.expected-files"
cmp "$2.files" "$2.expected-files"
find . -type d -printf '%P\n' | sed '/^$/d' | LC_ALL=C sort > "$2.dirs"
LC_ALL=C sort "$4" > "$2.expected-dirs"
cmp "$2.dirs" "$2.expected-dirs"
'@
    $check = $check.Replace("`r`n", "`n")
    Invoke-NameNode @('bash', '-c', $check, 'integrite', $HdfsPath, "$nodeTemp/$Label", "$nodeTemp/runtime.sha256", "$nodeTemp/runtime.dirs") | Out-Null
}

function Test-Replication {
    param([string] $HdfsPath)

    $report = (Invoke-NameNode @('hdfs', 'fsck', $HdfsPath, '-files', '-blocks')) -join "`n"
    $replicas = [regex]::Matches($report, 'Live_repl=(\d+)')
    if ($report -notmatch '(?m)^Status:\s+HEALTHY' -or $replicas.Count -eq 0 -or
        @($replicas | Where-Object { [int] $_.Groups[1].Value -ne 2 }).Count -gt 0) {
        throw "Réplication HDFS invalide à $HdfsPath. Chaque bloc doit avoir deux répliques saines."
    }
    return $replicas.Count
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw 'Docker Compose est requis.'
}
New-Item -ItemType Directory -Path $temp | Out-Null
if ($FromRelease) {
    if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
        throw "GitHub CLI est requis pour -FromRelease. Télécharger les deux fichiers sur https://github.com/$repository/releases/tag/$release puis utiliser -ArchivePath."
    }
    Invoke-Checked -Program 'gh' -CommandArgs @(
        'release', 'download', $release, '--repo', $repository, '--dir', $temp,
        '--pattern', $archiveName, '--pattern', "$archiveName.sha256"
    ) | Out-Null
    $ArchivePath = Join-Path $temp $archiveName
} elseif (-not $ArchivePath) {
    $ArchivePath = Join-Path $repoRoot "shared_data/delivery/$archiveName"
}
if (-not (Test-Path -LiteralPath $ArchivePath -PathType Leaf)) {
    throw "Archive introuvable à $ArchivePath. Fournir -ArchivePath ou -FromRelease."
}
$zipPath = (Resolve-Path -LiteralPath $ArchivePath).Path
if (-not (Test-Path -LiteralPath "$zipPath.sha256" -PathType Leaf)) {
    throw "Fichier compagnon manquant : $zipPath.sha256."
}
$checksumLine = (Get-Content -LiteralPath "$zipPath.sha256" -Raw).Trim()
if ($checksumLine -notmatch '^([0-9a-fA-F]{64})(?:\s+[^\r\n]+)?$') {
    throw "Le fichier $zipPath.sha256 ne contient pas une empreinte SHA-256 valide."
}
$archiveHash = (Get-FileHash -LiteralPath $zipPath -Algorithm SHA256).Hash.ToLowerInvariant()
if ($archiveHash -ne $Matches[1]) {
    throw "SHA-256 de l’archive invalide : $zipPath."
}

Add-Type -AssemblyName System.IO.Compression.FileSystem
$archive = [IO.Compression.ZipFile]::OpenRead($zipPath)
try {
    $paths = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($entry in $archive.Entries) {
        $name = $entry.FullName.TrimEnd('/')
        if ($name -cnotmatch "^$version/[A-Za-z0-9_.+/-]+$" -or
            $name -match '(?:^|/)\.{1,2}(?:/|$)|//' -or -not $paths.Add($name)) {
            throw "Entrée d’archive invalide ou en double : $name."
        }
    }
} finally {
    $archive.Dispose()
}
Expand-Archive -LiteralPath $zipPath -DestinationPath $temp
$bundle = Join-Path $temp $version
$hashes = [Collections.Generic.Dictionary[string,string]]::new([StringComparer]::Ordinal)
foreach ($line in Get-Content -LiteralPath (Join-Path $bundle 'SHA256SUMS.txt')) {
    if ($line -cnotmatch '^([0-9a-f]{64})  ([A-Za-z0-9_.+/-]+)$') {
        throw 'Ligne invalide dans SHA256SUMS.txt.'
    }
    $hash, $path = $Matches[1], $Matches[2]
    if ($path -match '(?:^|/)\.{1,2}(?:/|$)|//' -or $path.StartsWith('/') -or $path -eq 'SHA256SUMS.txt') {
        throw "Chemin invalide dans le manifeste d’intégrité : $path."
    }
    $hashes.Add($path, $hash)
    $file = Join-Path $bundle $path
    if (-not (Test-Path -LiteralPath $file -PathType Leaf) -or (Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash -ne $hash) {
        throw "Empreinte incorrecte pour $path."
    }
}
$files = @(Get-ChildItem -LiteralPath $bundle -Force -Recurse -File)
if ($files.Count -ne $hashes.Count + 1) {
    throw "Le ZIP contient des fichiers absents de son manifeste d’intégrité."
}
$required = @(
    'manifest/part-00000',
    'pipelines/j1_log/metadata/part-00000',
    'pipelines/j2_v2/metadata/part-00000',
    'replay/receipts.csv',
    'replay/expected.csv'
)
foreach ($name in $required) {
    if (-not $hashes.ContainsKey($name)) {
        throw "Paquet incomplet : $name."
    }
}
$manifest = Get-Content -LiteralPath (Join-Path $bundle 'manifest/part-00000') -Raw | ConvertFrom-Json
if ($manifest.policy_version -ne $version -or
    $manifest.routes.'1'.pipeline -ne 'pipelines/j1_log' -or
    $manifest.routes.'2'.pipeline -ne 'pipelines/j2_v2') {
    throw 'Politique ou routes des pipelines incompatibles.'
}

$runtime = Join-Path $temp 'runtime'
New-Item -ItemType Directory -Path $runtime | Out-Null
foreach ($folder in 'manifest', 'pipelines', 'replay') {
    Copy-Item -LiteralPath (Join-Path $bundle $folder) -Destination $runtime -Recurse
}
$runtimeLines = @(
    $hashes.Keys | Where-Object { $_ -match '^(manifest|pipelines|replay)/' } | Sort-Object |
        ForEach-Object { "$($hashes[$_])  $_" }
)
$runtimeDirs = @(
    Get-ChildItem -LiteralPath $runtime -Directory -Recurse |
        ForEach-Object { $_.FullName.Substring($runtime.Length + 1).Replace('\', '/') }
)
[IO.File]::WriteAllText((Join-Path $temp 'runtime.sha256'), (($runtimeLines -join "`n") + "`n"), [Text.Encoding]::ASCII)
[IO.File]::WriteAllText((Join-Path $temp 'runtime.dirs'), (($runtimeDirs -join "`n") + "`n"), [Text.Encoding]::ASCII)

Push-Location $repoRoot
try {
    Invoke-NameNode @('hdfs', 'dfs', '-test', '-d', '/') | Out-Null
    $replication = ((Invoke-NameNode @('hdfs', 'getconf', '-confKey', 'dfs.replication')) -join "`n").Trim()
    if ($replication -ne '2') {
        throw "Le NameNode annonce dfs.replication=$replication, valeur attendue : 2."
    }
    Invoke-NameNode @('mkdir', $nodeTemp) | Out-Null
    foreach ($name in 'runtime.sha256', 'runtime.dirs') {
        Invoke-Checked -Program 'docker' -CommandArgs @('compose', 'cp', (Join-Path $temp $name), "namenode:$nodeTemp/$name") | Out-Null
    }
    $exists = Invoke-NameNode @(
        'bash', '-c', 'if hdfs dfs -test -e "$1"; then echo present; else echo absent; fi',
        'presence', $target
    )
    $mode = 'reused'
    if (@($exists) -notcontains 'present') {
        $mode = 'installed'
        $parent = $target.Substring(0, $target.LastIndexOf('/'))
        $staged = "$parent/.ml-install-$id"
        Invoke-NameNode @('hdfs', 'dfs', '-mkdir', '-p', $parent) | Out-Null
        Invoke-Checked -Program 'docker' -CommandArgs @('compose', 'cp', $runtime, "namenode:$nodeTemp/runtime") | Out-Null
        Invoke-NameNode @('hdfs', 'dfs', '-put', "$nodeTemp/runtime", $staged) | Out-Null
        Test-ImportedFiles $staged 'staged'
        Test-Replication $staged | Out-Null
        # RENAME avec l'option NONE rejette une destination créée entre-temps.
        Invoke-NameNode @(
            'curl', '--fail', '--silent', '--show-error', '--max-time', '30', '--get', '-X', 'PUT',
            '--data-urlencode', 'op=RENAME', '--data-urlencode', 'user.name=hadoop',
            '--data-urlencode', 'renameoptions=NONE', '--data-urlencode', "destination=$target",
            "http://localhost:9870/webhdfs/v1$staged"
        ) | Out-Null
    }
    try {
        Test-ImportedFiles $target 'final'
    } catch {
        throw "Le contenu HDFS à $target ne correspond pas au paquet vérifié. $($_.Exception.Message)"
    }
    foreach ($folder in 'pipelines/j1_log', 'pipelines/j2_v2') {
        Invoke-NameNode @('hdfs', 'dfs', '-test', '-d', "$target/$folder") | Out-Null
    }
    $blocks = Test-Replication $target
    Write-Output "Paquet $version prêt. État : $mode. Blocs répliqués deux fois : $blocks."
    Write-Output "URI HDFS : hdfs://namenode:9000$target"
    Write-Output "SHA-256 ZIP : $archiveHash"
} finally {
    Pop-Location
}
