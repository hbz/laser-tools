# Integrationsskript für Nationallizenzdaten

Dieses Script dient der Synchronisation von National- und Allianzlizenz-Paketen mit der GOKb.

Es werden (je nach Parameter) mehrere Schritte durchlaufen:

1. Import der Sigelinformationen aus dem Nationallizenzen-CMS
2. Anreicherung der Paketinformationen mit Daten aus dem ZDB-Sigelverzeichnis
3. Extrahieren von PICA+-Titeldaten über die SRU-Schnittstelle des GBV
4. Upload der Paket- und Titeldaten in eine GOKb-Instanz

Unterstützte Parameter:

--packages ["data_source,username,password"]
* erstellt die Datei mit CMS-Paketdaten
* werden keine Verbindungsinformationen angegeben, werden diese der Konfigurationsdatei 'login.json' entnommen.

--json [ZDB-1-...]
* erstellt Titel- und Paketdaten in GOKb-Integration-JSON
* Die Datei mit CMS-Paketdaten muss vorhanden sein.
* Ohne folgendes Paketsigel werden alle Pakete abgearbeitet. (In diesem Fall werden lokal nur Warnings generiert)

--endpoint ['zdb'|'natliz'|'gvk']
* ändert die Datenquelle für Titeldaten
* weglassen für Standardbezug über GVK-SRU

-- pub_type ['journal'|'book'|'all']
* Schränkt die verarbeitete Materialart ein
* Mögliche Werte: 'journal' (Standard), 'book', 'all'

--post [URL]
* Sendet die erschlossenen Daten an eine GOKb-Instanz
* Folgt keine URL, wird 'http://localhost:8080/gokb/' als Standardadresse verwendet.
* Nur zulässig im Anschluss an --json
* Die GOKb-Zugangsdaten werden der 'login.json' entnommen. Falls keine gefunden werden, wird nach ihnen gefragt.

--new_orgs
* überträgt gefundene Körperschaften mit GND-ID an die GOKb
* funktioniert nur in Verbindung mit --post

--local_pkg
* Statt dem Datenbezug über die ZDB wird ein bereits lokal im GOKb-JSON-Format vorhandenes Paket und dessen Titeldaten an die GOKb geschickt
* nur zulässig in Verbindung mit '--post' UND '--json' + Sigel
* Dateiname Titel: "titles_[SIGEL]_[endpoint].json"
* Dateiname Paket: "[SIGEL]_[endpoint].json"

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
