param(
    [Parameter(Mandatory = $true)]
    [string]$Source
)

$ErrorActionPreference = 'Stop'
$sourcePath = (Resolve-Path -LiteralPath $Source).Path
$datasets = @('ml_dataset', 'daily_metrics', 'hackernews_metrics', 'google_trends_metrics')
$hdfsRoot = '/trendcast/analytics'

function Invoke-Docker {
    $result = & docker @args
    if ($LASTEXITCODE -ne 0) {
        throw "Docker ou HDFS a échoué avec le code $LASTEXITCODE pour $($args -join ' ')"
    }
    $result
}

function Invoke-NameNode {
    Invoke-Docker compose exec -T namenode @args
}

function Confirm-HdfsFiles($hdfsDirectory, $files) {
    $targetNames = @(
        Invoke-NameNode hdfs dfs -ls -C $hdfsDirectory |
            ForEach-Object { ($_ -split '/')[-1] }
    )
    if (Compare-Object -ReferenceObject @($files.Name) -DifferenceObject $targetNames) {
        throw "Le contenu HDFS diffère de l'export dans $hdfsDirectory"
    }
    foreach ($file in $files) {
        $localHash = (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash
        $remoteHash = Invoke-NameNode bash -o pipefail -c 'hdfs dfs -cat "$1" | sha256sum' gold "$hdfsDirectory/$($file.Name)"
        if ($localHash -ne ($remoteHash -split '\s+')[0]) {
            throw "Empreinte différente pour $hdfsDirectory/$($file.Name)"
        }
    }
}

$goldDirectories = @(Invoke-NameNode hdfs dfs -ls -C $hdfsRoot)
Write-Output "Chemins HDFS existants sous $hdfsRoot"
$goldDirectories | Write-Output
$filesByDataset = @{}
$missing = @()
foreach ($dataset in $datasets) {
    $datasetPath = Join-Path $sourcePath $dataset
    $files = @(Get-ChildItem -LiteralPath $datasetPath -File)
    if (-not ($files | Where-Object Extension -eq '.parquet')) {
        throw "Aucun fichier Parquet dans $datasetPath"
    }
    if ($files | Where-Object { $_.Name -ne '_SUCCESS' -and $_.Name -notmatch '^part-[\w.-]+\.parquet$' }) {
        throw "Fichier inattendu dans $datasetPath"
    }
    $filesByDataset[$dataset] = $files
    $destination = "$hdfsRoot/$dataset"
    if ($goldDirectories -contains $destination) {
        Confirm-HdfsFiles $destination $files
        Write-Output "Déjà présent et vérifié $destination"
    } else {
        $missing += $dataset
    }
}

if ($missing.Count -gt 0) {
    $id = [guid]::NewGuid().ToString('N')
    $containerStaging = "/tmp/trendcast-gold-$id"
    $hdfsStaging = "$hdfsRoot/.gold-import-$id"
    $container = Invoke-Docker compose ps -q namenode
    Invoke-NameNode mkdir $containerStaging
    Invoke-NameNode hdfs dfs -mkdir $hdfsStaging
    foreach ($dataset in $missing) {
        Invoke-Docker cp (Join-Path $sourcePath $dataset) "${container}:${containerStaging}/"
        Invoke-NameNode hdfs dfs -put "$containerStaging/$dataset" "$hdfsStaging/"
        Confirm-HdfsFiles "$hdfsStaging/$dataset" $filesByDataset[$dataset]
    }
    foreach ($dataset in $missing) {
        # The final path is still absent immediately before the HDFS rename.
        $destination = "$hdfsRoot/$dataset"
        if (@(Invoke-NameNode hdfs dfs -ls -C $hdfsRoot) -contains $destination) {
            throw "Le chemin HDFS est apparu pendant l'import $destination"
        }
        Invoke-NameNode hdfs dfs -mv "$hdfsStaging/$dataset" $destination
        Write-Output "Importé et vérifié $destination"
    }
}

foreach ($dataset in $datasets) {
    Invoke-NameNode hdfs fsck "$hdfsRoot/$dataset" -files -blocks -locations
}
