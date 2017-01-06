# Integrationsskript für Nationallizenzdaten

Dieses Script dient der Synchronisation von Nationallizenz-Paketen mit der GOKb.

Es werden (je nach Parameter) mehrere Schritte durchlaufen:

1. Import der Sigelinformationen aus dem Nationallizenzen-CMS
2. Anreicherung der Paketinformationen mit Daten aus dem ZDB-Sigelverzeichnis
3. Extrahieren von PICA+-Titeldaten über die SRU-Schnittstelle des GBV
4. Upload der Paket- und Titeldaten in eine GOKb-Instanz

Mögliche Parameter:

--packages ["data_source,username,password"]
* erstellt die Datei mit CMS-Paketdaten
* werden keine Verbindungsinformationen angegeben, werden diese der Konfigurationsdatei 'login.json' entnommen.

--json [ZDB-1-...]
* erstellt Titel- und Paketdaten
* Die Datei mit CMS-Paketdaten muss vorhanden sein.
* Ohne folgendes Paketsigel werden alle Pakete abgearbeitet.

--post [URL]
* Sendet die erschlossenen Daten an eine GOKb-Instanz
* Folgt keine URL, wird die localhost Standardadresse verwendet.
* Nur zulässig im Anschluss an --json
* Die GOKb-Zugangsdaten werden der 'login.json' entnommen. Falls keine gefunden werden, wird nach ihnen gefragt.

## Beispiel login.json

```JSON
{
  "cms" : {
    "base" : "dbi:Pg...",
    "username" : "...",
    "password" : "..."
  },
  "gokb" : {
    "base" : "http://localhost:8080/gokb/",
    "username" : "...",
    "password" : "...",
  }
}
```
