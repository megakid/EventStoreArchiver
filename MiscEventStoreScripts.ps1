<#

    Only do this process on eventstores that have previously been scavenged

#>

Set-StrictMode -Version latest
$ErrorActionPreference = "Stop"

$rebuildIndex = $false

$es = "E:\tmp\es"

#$src="\\Bl-svr153\d$\EventStore\Pinnacle"
$src="E:\tmp\pinnOrig"
$tmp="E:\tmp\pinn1"

$srcData = "$src\data"
$tmpLog = "$tmp\logs"
$tmpData = "$tmp\data"

$httphost = "localhost"
$httpport = 1114
$user = 'admin'
$pass = 'changeit'



Function WaitForEventStoreToBeReady(){
    param(
        [Parameter(Mandatory=$true)][string]$httpHost,
        [Parameter(Mandatory=$true)][string]$httpPort
    )
    while($true)
    {
        $url = "http://$($httpHost):$($httpPort)/info"
        $req = [system.Net.WebRequest]::Create($url)
        $req.Timeout = 1000;

        try {
            $res = $req.GetResponse()
            if ($res.StatusCode -eq 200) {
                
                $ms = (New-Object System.IO.MemoryStream)
                $res.GetResponseStream().CopyTo($ms);
                $resj = ConvertFrom-Json ([System.Text.Encoding]::ASCII.GetString($ms.ToArray()))

                if ($resj.state -eq "master") {
                    Write-Output "Node detected as online..."
                    break
                }
            } 
            Write-Output "Node still offline..."
            
            Start-Sleep -Seconds 1
        } 
        catch {
            Write-Output "Error: $($_.Exception.Message)"
            Write-Output "Node still offline..."
        }
    }
}



Function SendAuthenticatedEmptyPostToEventStore(){
    param(
        [Parameter(Mandatory=$true)][string]$httpHost,
        [Parameter(Mandatory=$true)][string]$httpPort,
        [Parameter(Mandatory=$true)][string]$path,
        [Parameter(Mandatory=$false)][string]$user = "admin",
        [Parameter(Mandatory=$false)][string]$pass = "changeit"
    )
    $url = "http://$($httpHost):$($httpPort)/$($path)"
    $req = [system.Net.WebRequest]::Create($url)
    $req.Timeout = 20000;
    $req.Method = "POST"
    $req.Accept = "application/json";
    $req.ContentLength = 0;

    $encodedCreds = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes("$($user):$($pass)"))
    $basicAuthValue = "Basic $encodedCreds"

    $req.PreAuthenticate = $true;
    $req.Headers.Add("Authorization", $basicAuthValue);

    try {
        $res = $req.GetResponse()
        Write-Output "$($res.StatusCode) code back from $($path) request" 
        #}
    } catch {
        Write-Output "Error: $($_.Exception.Message)"
    }
}

Function TriggerEventStoreScavenge(){
    param(
        [Parameter(Mandatory=$true)][string]$httpHost,
        [Parameter(Mandatory=$true)][string]$httpPort,
        [Parameter(Mandatory=$false)][int]$startChunkNum = 0,
        [Parameter(Mandatory=$false)][int]$threads = 1,
        [Parameter(Mandatory=$false)][string]$user = "admin",
        [Parameter(Mandatory=$false)][string]$pass = "changeit"
    )

    $path = "admin/scavenge?startFromChunk=$($startChunkNum)&threads=$($threads)"

    SendAuthenticatedEmptyPostToEventStore $httpHost $httpPort $path $user $pass
}


Function TriggerEventStoreShutdown(){
    param(
        [Parameter(Mandatory=$true)][string]$httpHost,
        [Parameter(Mandatory=$true)][string]$httpPort,
        [Parameter(Mandatory=$false)][string]$user = "admin",
        [Parameter(Mandatory=$false)][string]$pass = "changeit"
    )
    $path = "admin/shutdown"

    SendAuthenticatedEmptyPostToEventStore $httpHost $httpPort $path $user $pass
}

Function WaitForEventStoreToFinishScavenging(){
    param(
        [Parameter(Mandatory=$true)][string]$logDir
    )

    $regex = "SCAVENGING: total time taken"

    while ($true) 
    {
        $file = @(gci -r $logDir\*cluster-node.log | sort LastWriteTime -Descending)[0]
        
        $success = $false
        
        foreach($line in Get-Content $file) {
            if($line -match $regex){
                $success = $true
                break
            }
        }

        if ($success)
        {
            Write-Output "Searching $($file) ... Found SCAVENGE COMPLETED message"
            break
        }

        Write-Output "Searching $($file) ... Not found SCAVENGE COMPLETED message - sleeping for 30 secs"
        Start-Sleep -Seconds 30
    }
}

Function LaunchEventStore(){
    param(
        [Parameter(Mandatory=$true)][string]$esPath,
        [Parameter(Mandatory=$true)][string]$httpPort,
        [Parameter(Mandatory=$true)][string]$dataDir,
        [Parameter(Mandatory=$true)][string]$logDir
    )

    $esBinary = "$esPath\clusternode\EventStore.ClusterNode.exe"
    $args = "-Db $dataDir --log $logDir --int-ip 127.0.0.1 --ext-ip 127.0.0.1 --int-tcp-port=1111 --ext-tcp-port=1112 --int-http-port=1113 --ext-http-port=$httpPort --RunProjections=None  -SkipIndexVerify -SkipIndexScanOnReads " # -DisableScavengeMerging"

    Write-Output $esBinary $args

    Start-Process $esBinary $args

}
Function CopyIndexes(){
    param(
        [Parameter(Mandatory=$true)][string]$src,
        [Parameter(Mandatory=$true)][string]$dst
    )
    
    robocopy *indexmap* $src\index $dst\index /copyall /mir

    robocopy $src\index $dst\index /copyall /mir /xf indexmap

}

Function CopyDatabase() {
    param(
        [Parameter(Mandatory=$true)][string]$src,
        [Parameter(Mandatory=$true)][string]$dst
    )

    # checkpoint files first
    robocopy *.chk $src $dst /copyall /mir

    # could take a long time:
    CopyIndexes $src $dst
    
    # could take a long time:
    robocopy chunk* $src $dst /copyall /mir

}

Function CopyScavengedChunks(){
    param(
        [Parameter(Mandatory=$true)][string]$src,
        [Parameter(Mandatory=$true)][string]$dst
    )

    # now copy back any new files (/xo /xn) to origin...
    robocopy chunk* $src $dst /copyall /xo /xn /xf *.000000

}



#####

CopyDatabase $srcData $tmpData

# clear logs
Remove-Item –Path $tmpLog –Recurse -ErrorAction SilentlyContinue


LaunchEventStore $es $httpport $tmpData $tmpLog

# wait for cluster to come up...
WaitForEventStoreToBeReady $httphost $httpport
 

 Write-Output "Sleeping for 2 secs..."
 Start-Sleep -Seconds 2

if ($scavenge) {
 
    # now run a scavenge
    TriggerEventStoreScavenge $httphost $httpport

    # wait for cluster to finish scavenging...
    WaitForEventStoreToFinishScavenging $tmpLog
    
    Write-Output "Sleeping for 10 secs..."
    Start-Sleep -Seconds 10

    # shutdown node gracefully
    TriggerEventStoreShutdown $httphost $httpport

    Write-Output "Sleeping for 10 secs..."
    Start-Sleep -Seconds 10

    
    # now copy back to origin...

    # could take a long time:
    CopyScavengedChunks $tmpData $srcData

}

#### OPTIONAL - rebuild indexes
if ($rebuildIndex) {

    Write-Output "Rebuilding index..."
    
    Remove-Item –Path $tmpData\index –Recurse

    LaunchEventStore $es $httpport $tmpData $tmpLog
    
    # wait for cluster to come up again...
    WaitForEventStoreToBeReady $httphost $httpport
 
     Write-Output "Sleeping for 10 secs..."
     Start-Sleep -Seconds 10
    
    # shutdown node gracefully
    TriggerEventStoreShutdown $httphost $httpport
    
    Write-Output "Rebuilt index..."

    Write-Output "Sleeping for 10 secs..."
    Start-Sleep -Seconds 10

    # COPY INDEX BACK
    # could take a long time:
    CopyIndexes $tmpData $srcData

}

#### END INDEX REBUILD


# now copy back to origin...

# could take a long time:
CopyScavengedChunks $tmpData $srcData


# old chunks are automatically removed on next node startup/restart - EVEN WORKS with merged chunks
<#
08772,01,19:21:37.132] Opened completed E:\tmp\pinnOrig\data\chunk-000017.000001 as version 3
[08772,01,19:21:37.132] Opened completed E:\tmp\pinnOrig\data\chunk-000018.000001 as version 3
[08772,01,19:21:37.362] Opened ongoing E:\tmp\pinnOrig\data\chunk-000019.000000 as version 3
[08772,01,19:21:37.364] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000000.000000...
[08772,01,19:21:37.364] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000001.000000...
[08772,01,19:21:37.364] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000002.000000...
[08772,01,19:21:37.364] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000003.000000...
[08772,01,19:21:37.364] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000004.000000...
[08772,01,19:21:37.364] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000005.000000...
[08772,01,19:21:37.364] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000006.000000...
[08772,01,19:21:37.380] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000007.000000...
[08772,01,19:21:37.380] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000008.000000...
[08772,01,19:21:37.380] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000009.000000...
[08772,01,19:21:37.380] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000010.000000...
[08772,01,19:21:37.380] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000011.000000...
[08772,01,19:21:37.380] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000012.000000...
[08772,01,19:21:37.380] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000013.000000...
[08772,01,19:21:37.380] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000014.000000...
[08772,01,19:21:37.380] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000015.000000...
[08772,01,19:21:37.380] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000016.000000...
[08772,01,19:21:37.380] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000017.000000...
[08772,01,19:21:37.380] Removing excess chunk version: E:\tmp\pinnOrig\data\chunk-000018.000000...
#>