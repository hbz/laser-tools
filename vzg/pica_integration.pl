#!/usr/bin/perl

# Dieses Script dient der Synchronisation von NL-Paketen mit der GOKb
# Es werden (je nach Aufruf) mehrere Schritte durchlaufen:
# 1. Import der Sigelinformationen aus dem Nationallizenzen-CMS
# 2. Anreicherung der Paketinformationen mit Daten aus dem ZDB-Sigelverzeichnis
# 3. Extrahieren von Titelinformationen über eine PICA-XML SRU-Schnittstelle
# 4. Upload der Paket- und Titeldaten in eine GOKb-Instanz
#
# Parameter:
# --packages "data_source,username,password"
#  * erstellt die JSON-Datei mit CMS-Paketinformationen
# --json (ZDB-1-...)
#  * generiert JSON für das angegebene Paket
#  * Datei mit CMS-Paketinformationen muss vorhanden sein
#  * ohne folgendes Paketsigel werden alle (relevaten) Pakete in der Datei mit CMS-Paketinformationen verarbeitet.
# --endpoint
#  * ändert die Datenquelle für Titeldaten
#  * weglassen für Standardbezug über VZG-SRU
#  * Mögliche Werte: "zdb","natliz","gvk" (Standard), "fid", "gbvcat" (Zugriffsbeschränkt), "ebp" (Zugriffbeschränkt)
# --post (URL)
#  * überträgt die ausgewählten Pakete an eine GOKb-Instanz
#  * folgt keine URL, wird die localhost Standard-Adresse verwendet
#  * nur zulässig nach --json
# --new_orgs
#  * überträgt gefundene Körperschaften mit GND-ID an die GOKb
#  * funktioniert nur in Verbindung mit --post
# --pub_type (Materialart)
#  * Schränkt die verarbeitete Materialart ein
#  * Mögliche Werte: 'all' (Standard), 'book', 'journal'
# --local_pkg
#  * Statt dem Datenbezug über die ZDB wird ein bereits lokal im GOKb-JSON-Format vorhandenes Paket und dessen Titeldaten an die GOKb geschickt
#  * nur zulässig in Verbindung mit --post UND --json mit Sigel
#  * Dateiname Titel: "./titles/titles_[SIGEL]_[endpoint].json"
#  * Dateiname Paket: "./packages/[SIGEL]_[endpoint].json"
# --pkg_owner
#  * Bestimmt den letzten Teil des Paketnamens
# --login_file
#  * Spezifiziert einen abweichenden Namen einer JSON-Datei mit Login-Informationen

use v5.22;
use strict;
use warnings;
use utf8;
use DBI;
use JSON;
use URI;
use Unicode::Normalize;
use IO::Tee;
use Log::Log4perl;
use Time::Duration;
binmode(STDOUT, ":utf8");

use POSIX qw(strftime);
use File::Copy;
use Path::Class;
use Scalar::Util qw(looks_like_number);
use List::MoreUtils qw(first_index any none);
use LWP::UserAgent;
use HTTP::Request;
use HTTP::Request::Common;
use Term::ReadKey;
use WWW::Mechanize;

use Catmandu -all;
use Catmandu::PICA;
use Catmandu::Importer::SRU;
use Catmandu::Importer::SRU::Parser::ppxml;
use Catmandu::Importer::SRU::Parser::picaxml;
use Data::Dumper;
use PICA::Data ':all';
use Algorithm::CheckDigits;

# Config

## Output directories

my $packageDir = dir("packages");
my $titleDir = dir("titles");
my $warningDir = dir("warnings");
my $orgsDir = dir("orgs");
my $logDir = dir("logs");

## userListVerifier in der GOKb setzen?

my $verifyTitleList = 0;
my @allTitles;

## Nur Zeitschriften?

my $requestedType = "all";

## Name der JSON-Datei mit Paketinformationen

my $knownSeals = 'CMS_Pakete.json';

## Standard-URL der Ziel-GOKb

my $baseUrl = 'http://localhost:8080/gokb/';
my $filter;


### logging

my $conf = q(

  log4perl.logger                    = DEBUG, Logfile, Screen

  log4perl.filter.MatchFile      = Log::Log4perl::Filter::LevelRange
  log4perl.filter.MatchFile.LevelMin = INFO
  log4perl.filter.MatchFile.LevelMax = FATAL
  log4perl.filter.MatchFile.AcceptOnMatch = true
  log4perl.appender.Logfile          = Log::Log4perl::Appender::File
  log4perl.appender.Logfile.Filter   = MatchFile
  log4perl.appender.Logfile.filename = logs/test.log
  log4perl.appender.Logfile.layout   = Log::Log4perl::Layout::PatternLayout
  log4perl.appender.Logfile.layout.ConversionPattern = %d - %p (%L) -- %m%n

  log4perl.filter.MatchScreen      = Log::Log4perl::Filter::LevelRange
  log4perl.filter.MatchScreen.LevelMin = DEBUG
  log4perl.filter.MatchScreen.LevelMax = FATAL
  log4perl.filter.MatchScreen.AcceptOnMatch = true
  log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
  log4perl.appender.Screen.Filter  = MatchScreen
  log4perl.appender.Screen.stderr  = 0
  log4perl.appender.Screen.layout  = Log::Log4perl::Layout::PatternLayout
  log4perl.appender.Screen.layout.ConversionPattern = %d - %p (%L) -- %m%n
);

# ... passed as a reference to init()
Log::Log4perl::init( \$conf );

my $logger = Log::Log4perl->get_logger();

## Öffne JSON-Datei mit GOKb-Organisationsdaten

my $ncsu_orgs = do {
  open(my $orgs_in, '<' , "ONLD.jsonld")
      or $logger->logdie("Can't open ONLD.jsonld: $!");

  local $/;

  <$orgs_in>
};

my %orgsJSON = %{decode_json($ncsu_orgs)}
  or $logger->logdie("Konnte JSON mit NCSU-Orgs nicht dekodieren!");

my $matchOrgsByFile = 0;

# Check for login configuration

my %cmsCreds;
my %gokbCreds;

# Handle parameters

my $endpoint = "gvk";
my $newOrgs = 0;
my $localPkg = 0;
my %resolvedPlatform;
my $customTarget;
my $owner = "";

my $argP = first_index { $_ eq '--packages' } @ARGV;
my $argJ = first_index { $_ eq '--json' } @ARGV;
my $argPost = first_index { $_ eq '--post' } @ARGV;
my $argEndpoint = first_index { $_ eq '--endpoint' } @ARGV;
my $argNewOrgs = first_index { $_ eq '--new_orgs' } @ARGV;
my $argType = first_index { $_ eq '--pub_type' } @ARGV;
my $argLocal = first_index { $_ eq '--local_pkg' } @ARGV;
my $argOwner = first_index { $_ eq '--pkg_owner' } @ARGV;
my $argCreds = first_index { $_ eq '--login_file' } @ARGV;

my $loginFn = $argCreds >= 0 ? $ARGV[$argCreds+1] : 'login.json';

if(-e $loginFn){
  my $login_data = do {
    open(my $logins, '<' , $loginFn)
        or $logger->logdie("Can't open login file: $!");

    local $/;

    <$logins>
  };

  my %logins = %{decode_json($login_data)}
    or $logger->logdie("Konnte JSON mit Logins nicht dekodieren!");

  if($logins{'cms'}){
    %cmsCreds = %{ $logins{'cms'} };
  }

  if($logins{'gokb'}){
    %gokbCreds = %{ $logins{'gokb'} };
  }
}

if($ARGV[$argPost+1] && index($ARGV[$argPost+1], "http") == 0){
  $gokbCreds{'base'} = $ARGV[$argPost+1];
  $customTarget = 1;
}

if($argOwner >= 0 && $ARGV[$argOwner+1] && index($ARGV[$argOwner+1], "--") == -1){
  $owner = $ARGV[$argOwner+1];
}

if( $argType >= 0) {
  if($ARGV[$argType+1] && any { $_ eq $ARGV[$argType+1] } ("journal","book","all") ) {
    $requestedType = $ARGV[$argType+1];
  }
  else{
    $logger->logdie("Ungültiger Materialtyp! Möglich sind 'journal'(Standard), 'book' und 'all'");
  }
}

if($argNewOrgs >= 0){
  $newOrgs = 1;
}
if($argLocal >= 0){
  $localPkg = 1;
}

if($argEndpoint >= 0) {
  if($ARGV[$argEndpoint+1] && any { $_ eq $ARGV[$argEndpoint+1] } ("zdb","natliz","gvk","gbvcat","fid","ebp") ) {
    $endpoint = $ARGV[$argEndpoint+1];
  }
  else{
    $logger->logdie("Ungültiger Endpunkt! Möglich sind 'zdb', 'natliz', 'fid', 'gbvcat'(Zugriffsbeschränkt) und 'gvk'(Standard)");
  }
}

if(!$gokbCreds{'base'}){
  $gokbCreds{'base'} = $baseUrl;
}

if($argP >= 0){
  if($ARGV[$argP+1] && index($ARGV[$argP+1], "dbi") == 0){
    my @creds = split(",", $ARGV[$argP+1]);

    if(scalar @creds == 3){
      $cmsCreds{'base'} = $creds[0];
      $cmsCreds{'username'} = $creds[1];
      $cmsCreds{'password'} = $creds[2];
    }
    else{
      $logger->logdie("Falsches Format der DB-Daten! Abbruch!");
    }
  }

  if(!$cmsCreds{'base'} || !$cmsCreds{'username'} || !$cmsCreds{'password'}){
    $logger->logdie("Datenbankinformationen fehlen/falsch! Format ist: \"data_source,username,password\"");
  }

  if($argJ >= 0){
    if(getSeals($cmsCreds{'base'},$cmsCreds{'username'},$cmsCreds{'password'}) == 0){
      my $post = 0;

      if($argPost >= 0){
        if(!$gokbCreds{'username'} || !$gokbCreds{'password'} || ($customTarget && $customTarget == 1)){
          say STDOUT "GOKb-Benutzername:";

          $gokbCreds{'username'} = <STDIN>;

          say STDOUT "GOKb-Passwort:";

          ReadMode 2;

          $gokbCreds{'password'} = <STDIN>;

          ReadMode 0;
        }
        if($gokbCreds{'username'} && $gokbCreds{'password'}){
          $post = 1;
        }
        else{
          $logger->warn("Kein Benutzername/Passwort, überspringe GOKb-Import!");
        }
      }
      if(index($ARGV[$argJ+1], "ZDB") == 0){
        $filter = $ARGV[$argJ+1];
        createJSON($post, $endpoint, $newOrgs, $localPkg);
      }
      else{
        $logger->info("Pakete abgerufen, erstelle JSONs!");

        createJSON($post, $endpoint, $newOrgs, $localPkg);
      }
    }
    else{
      $logger->logdie("Erstelle keine JSONs, Sigeldatei wurde nicht erstellt!");
    }
  }
  else{
      $logger->info("Erstelle nur Paketdatei $knownSeals!");

      getSeals($cmsCreds{'base'},$cmsCreds{'username'},$cmsCreds{'password'});
  }
}
elsif($argJ >= 0){
  if(-e $knownSeals){
    my $post = 0;

    if($argPost >= 0){
      if(!$gokbCreds{'username'} || !$gokbCreds{'password'} || ($customTarget && $customTarget == 1) ){
        say STDOUT "GOKb-Benutzername:";

        $gokbCreds{'username'} = <STDIN>;

        $gokbCreds{'username'} =~ s/^[\s\n]+|[\s\n]+$//gm;

        say STDOUT "GOKb-Passwort:";

        ReadMode 2;

        $gokbCreds{'password'} = <STDIN>;

        ReadMode 0;

        $gokbCreds{'password'} =~ s/^[\s\n]+|[\s\n]+$//gm;
      }

      if($gokbCreds{'username'} && $gokbCreds{'password'}){
        $post = 1;
      }
      else{
        $logger->warn("Kein Benutzername/Passwort, überspringe GOKb-Import!");
      }
    }
    if($ARGV[$argJ+1] && index($ARGV[$argJ+1], "ZDB") == 0){
      $filter = $ARGV[$argJ+1];

      $logger->info("Paketdatei gefunden, erstelle JSON für $filter!");

      createJSON($post, $endpoint, $newOrgs, $localPkg);
    }
    else{
      $logger->info("Paketdatei gefunden, erstelle JSONs!");

      createJSON($post, $endpoint, $newOrgs, $localPkg);
    }
  }
  else{
    $logger->error("Paketdatei nicht vorhanden!");

    $logger->logdie("Zum Erstellen mit Parameter '--packages' starten!");
  }
}

# No parameters

if(scalar @ARGV == 0 || (!$argJ && !$argP)){

  say STDOUT "Keine Parameter gefunden!";
  say STDOUT "Mögliche Parameter sind:";
  say STDOUT "'--packages \"data_source,username,password\"'";
  say STDOUT "'--json [\"Sigel\"]'";
  say STDOUT "'--endpoint zdb|gvk|natliz|fid|ebp'";
  say STDOUT "'--post [\"URL\"]'";
  say STDOUT "'--new_orgs'";
  say STDOUT "'--pub_type journal|book|all'";
  say STDOUT "'--local_pkg'";
}

# Query Sigelverzeichnis via SRU for package metadata

sub getZdbName {
  my $sig = shift;

  $logger->info("Sigel: $sig");

  my %pkgInfos = (
    'name' => "",
    'type' => "",
    'provider' => "",
    'platform' => "",
    'mainUrl' => "",
    'authority' => "",
    'scope' => "",
    'numZDB' => 0,
    'numTotal' => 0
  );

  my %attrs = (
      base => 'https://services.dnb.de/sru/bib',
      query => 'dnb.isl='.$sig,
      recordSchema => 'PicaPlus-xml',
      parser => 'ppxml'
  );
  my $importer = Catmandu::Importer::SRU->new(%attrs)
    or die " - Abfrage über ".$attrs{'base'}." fehlgeschlagen!\n";

  $importer->each(
    sub {
      my $packageInstance = shift;

      if(pica_value($packageInstance, '035Ea') ne 'I'
        && pica_value($packageInstance, '008Hd') eq $sig
      ){
        $logger->debug("Valides Paket gefunden.");

        my $messyName = pica_value($packageInstance, '029Aa');
        my $bracketPos = index($messyName, '[');

        $logger->debug("Paketname: $messyName");

        if($bracketPos > 0){
          $pkgInfos{'name'} = substr($messyName, 0, $bracketPos-1);
        }
        else {
          $pkgInfos{'name'} = $messyName;
        }

        $pkgInfos{'name'} =~ s/^\s+|\s+$//g;

        $pkgInfos{'provider'} = pica_value($packageInstance, '035Pg')
          ? pica_value($packageInstance, '035Pg')
          : "";

        $pkgInfos{'type'} = pica_value($packageInstance, '035Pi')
          ? pica_value($packageInstance, '035Pi')
          : "";

        $pkgInfos{'provider'} =~ s/Anbieter: //;

        if(index($pkgInfos{'provider'}, ";") >= 0){
          ($pkgInfos{'provider'}, $pkgInfos{'platform'}) = split(";",$pkgInfos{'provider'});
          $pkgInfos{'provider'} =~ s/^\s+|\s+$//g;
          $pkgInfos{'platform'} =~ s/^\s+|\s+$//g;
        }

        if(pica_value($packageInstance, '035Pe') && looks_like_number(pica_value($packageInstance, '035Pe'))){
          $pkgInfos{'numTotal'} = pica_value($packageInstance, '035Pe');
        }

        if(pica_value($packageInstance, '035Pf') && looks_like_number(pica_value($packageInstance, '035Pf'))){
          $pkgInfos{'numZDB'} = pica_value($packageInstance, '035Pf');
        }

        if(pica_value($packageInstance, '009Qu')){
          my @pUrls = @{ pica_fields($packageInstance, '009Q') };

          foreach my $pUrl (@pUrls){
            my @pUrl = @{$pUrl};
            my $urlType;
            my $url;
            my $subfPos = 0;

            foreach my $subField (@pUrl){
              if($subField eq 'u'){
                $url = $pUrl[$subfPos+1];
              }
              elsif($subField eq 'z'){
                $urlType = $pUrl[$subfPos+1];
              }
              $subfPos++;
            }

            if($url && $urlType && $urlType eq 'A'){
              $pkgInfos{'mainUrl'} = pica_value($packageInstance, '009Qu');
            }
          }
        }

        if(index($pkgInfos{'provider'}, "(") >= 0){
          $pkgInfos{'provider'} = substr($pkgInfos{'provider'}, 0, index  ($pkgInfos{'provider'}, "(")-1);
        }

        $pkgInfos{'authority'} = pica_value($packageInstance, '032Pa')
          ? pica_value($packageInstance, '032Pa')
          : "";

        $pkgInfos{'scope'} = pica_value($packageInstance, '035Pa')
          ? pica_value($packageInstance, '035Pa')
          : "";

      }
      else{
        print strftime '%Y-%m-%d %H:%M:%S', localtime;
        print " - Überspringe Eintrag für ".$sig;
        print ": 035Ea: ".pica_value($packageInstance, '035Ea');
        print " - 008Hd: ".pica_value($packageInstance, '008Hd')." \n";
      }
    }
  );
  return %pkgInfos;
}

# Import ZDB seals via SQL and write them to JSON

sub getSeals {
  my ($host, $username, $password) = @_;
  my $dbh = DBI->connect(
      $host,
      $username,
      $password,
      {
        AutoCommit=>1,
        RaiseError=>1,
        PrintError=>0
      }
  );

  # Request package id, seal

  my $stmt = qq(select zuid,
    seal as sigel,
    meta_type as type
    FROM lmodels
    WHERE \( meta_type = 'NLLicenceModelStandard' OR meta_type = 'NLLicenceModelOptIn' \)
    AND wf_state='published';
  );

  # Request linked institutions

  my $orgStmt = qq(SELECT zobjects.licences.zuid,
    zobjects.nlinstitutions.title as institution,
    zobjects.nlinstitutions.sigel as sigel,
    zobjects.nlinstitutions.uid as uid
    FROM zobjects.licences, zobjects.nlinstitutions
    WHERE zobjects.licences.lmodel = ?
    AND zobjects.licences.wf_state = 'authorized'
    AND zobjects.licences.lowner::uuid=zobjects.nlinstitutions.zuid;
  );
  my $stO = $dbh->prepare( $orgStmt );
  my $sth = $dbh->prepare( $stmt );
  my $rv = $sth->execute() or die $DBI::errstr;
  my $JSON = JSON->new->utf8->canonical;
  my %alljson;
  my %knownIsils;

  if(-e $knownSeals){
    copy($knownSeals, $knownSeals."_last.json");
  }

  open( my $out, '>', $knownSeals )
    or die "Failed to open $knownSeals for writing";

  if($rv < 0){
    print $DBI::errstr;
  }

  # Process packages

  while(my @row = $sth->fetchrow_array()) {
    my ($zuid, $pkgSigel, $licence_type) = @row;

    if($pkgSigel && $pkgSigel =~ /^ZDB-[0-9]+-[a-zA-Z]+[0-9]*$/ && $pkgSigel ne 'ZDB-1-TEST'){
      my %pkgInfos = getZdbName($pkgSigel);
      my $lType;
      if($licence_type eq 'NLLicenceModelStandard'){
        $lType = "NL";
      }
      elsif($licence_type eq 'NLLicenceModelOptIn'){
        if ($pkgInfos{'type'} =~ /Allianz-Lizenz/ ) {
          $lType = "AL";
        }
        else{
          $lType = $owner;
        }
      }

      my %pkg = (
        'sigel' => $pkgSigel,
        'zuid' => $zuid,
        'type' => $lType ? $lType : "",
        'name' => $pkgInfos{'name'},
        'authority' => $pkgInfos{'authority'},
        'provider' => $pkgInfos{'provider'},
        'platform' => $pkgInfos{'platform'},
        'url' => $pkgInfos{'mainUrl'},
        'scope' => $pkgInfos{'scope'},
        'cmsOrgs' => [],
        'zdbOrgs' => [],
        'orgStats' => {
          'numValidSig' => 0,
          'numOhneSig' => 0,
          'numWrongSig' => 0
        },
        'numZDB' => $pkgInfos{'numZDB'},
        'numTotal' => $pkgInfos{'numTotal'}
      );

      ## Process linked Orgs

      my $orgs = $stO->execute($zuid) or die $DBI::errstr;

      while(my @orgRow = $stO->fetchrow_array()){
        my $orgZuid = $orgRow[0];
        my $orgName = $orgRow[1];
        my $orgSigel = $orgRow[2] ? $orgRow[2] : undef;
        my $orgWibID = $orgRow[3];
        my $isil = undef;
        my $hasError = 0;
        my $tempISIL;

        ### Verify Seals

        if($orgSigel){

          $tempISIL = $orgSigel;
          $tempISIL =~ s/^\s+|\s+$//g;
          $tempISIL =~ s/ü/ue/g;
          $tempISIL =~ s/ä/ae/g;
          $tempISIL =~ s/ö/oe/g;

          if($tempISIL =~ /^[\w\d]+[\/\s\-]?[\w\d]*$/){

            unless($tempISIL =~ /^DE-/){

              if($tempISIL =~ /^\w+[\/\s\-]?\d+\w*\/?\d?$/){

                $tempISIL =~ s/^(\w+)([\/\s\-]?)(\d+\/?\w*\/?\d?)$/$1$3/g;
                $tempISIL =~ s/\//-/g;

              }
              elsif($tempISIL =~ /^\d+\/?[\d\w]*/){

                if($tempISIL =~ /^\d+\w*$/){
                }
                elsif($tempISIL =~ /^\d+\/[\d\w]+$/){

                  $tempISIL =~ s/\//-/g;

                }
                else{

                  $hasError = 1;

                }
              }
              else{

                $hasError = 1;

              }
              if($hasError == 0){
                $tempISIL = "DE-".$tempISIL;
              }
            }

            if($hasError == 0){
              if(!$knownIsils{$tempISIL}){
                my %bibAttrs = (
                    base => 'http://sru.gbv.de/isil',
                    query => 'pica.isi='.$tempISIL,
                    recordSchema => 'picaxml',
                    parser => 'picaxml'
                );
                $logger->info("Suche nach ISIL: $tempISIL!");
                my $orgImporter = Catmandu::Importer::SRU->new(%bibAttrs)
                  or $logger->logdie("Abfrage über ".$bibAttrs{'base'}." fehlgeschlagen!");

                my $sruOrg = $orgImporter->first();

                if($sruOrg){
                  $pkg{'orgStats'}{'numValidSig'}++;
                  $isil = $tempISIL;
                  $knownIsils{$tempISIL} = 1;
                }
                else{
                  $pkg{'orgStats'}{'numWrongSig'}++;
                  $logger->warn("Suche nach $tempISIL erfolglos!");
                  $knownIsils{$tempISIL} = 0;
                }
              }
              elsif($knownIsils{$tempISIL} == 1){
                $isil = $tempISIL;
              }
            }
            else{
              $pkg{'orgStats'}{'numWrongSig'}++;
              $logger->warn("Sigel $orgSigel ist offensichtlich ungültig.");
            }
          }
          else{
            $pkg{'orgStats'}{'numWrongSig'}++;
          }
        }

        if(!$orgSigel){
          $pkg{'orgStats'}{'numOhneSig'}++;
        }

        push @{ $pkg{'cmsOrgs'} }, {
            'name' => $orgName,
            'sigel' => $orgSigel,
            'isil' => $isil,
            'wibID' => $orgWibID,
            'zuid' => $orgZuid
        };
      }
      $pkg{'orgStats'}{'numCms'} = scalar @{ $pkg{'cmsOrgs'} };

      my %zdbAttrs = (
          base => 'http://services.dnb.de/sru/zdb',
          query => 'pica.isil='.$pkgSigel,
          recordSchema => 'PicaPlus-xml',
          _max_results => 1,
          parser => 'ppxml'
      );

      my $titleImporter = Catmandu::Importer::SRU->new(%zdbAttrs)
        or $logger->logdie("Abfrage über ".$zdbAttrs{'base'}." fehlgeschlagen!");
      my $zdbTitle = $titleImporter->first();

      if(ref($zdbTitle) eq 'HASH'){
        my @zdbHoldings = @{ pica_holdings($zdbTitle) };
        foreach my $zdbOrg (@zdbHoldings){
          if(pica_value($zdbOrg, '247CT')){
            push @{ $pkg{'zdbOrgs'} }, pica_value($zdbOrg, '247CT');
          }
        }
        $pkg{'orgStats'}{'numZdb'} = scalar @{ $pkg{'zdbOrgs'}};
      }

      if($pkg{'name'} ne ""){
        $alljson{$pkgSigel} = \%pkg;
      }

      sleep 1;
    }
    else{
      $logger->error("Kein Paketsigel oder falsches Format in zuid: $zuid.");
    }
  };
  $dbh->disconnect;

  say $out $JSON->pretty(1)->encode( \%alljson );

  close($out);
  sleep 1;
  return 0;
}

# Create packages, tipps and titles as GOKb-JSON (and trigger upload if requested)

my %globalIDs;
my @unknownRelIds;
my %orgsToAdd;
my %allISSN;
my %known;
my %checkedUrls;

sub createJSON {
  my $postData = shift;
  my $endpoint = shift;
  my $newOrgs = shift;
  my $localPkg = shift;

  if($localPkg && !$filter){
    $logger->logdie("Für die Verwendung bestehender JSON-Dateien muss ein Paketsigel hinter '--json' angegeben werden!");
  }

  my $json_seals = do {
    open(my $json_fh, '<' , $knownSeals)
        or $logger->logdie("Can't open \$knownSeals\": $!");
    local $/;
    <$json_fh>
  };

  # Input JSON handling

  %known = %{decode_json($json_seals)}
    or $logger->logdie("JSON nicht vorhanden!");
  my %knownSelection;

  if($filter){
    if($known{$filter}){
      $knownSelection{$filter} = $known{$filter};

      $logger->info("Generating JSON only for $filter!");
    }
    else{
      $logger->warn("Paket nicht bekannt! Suche Metadaten über Sigelstelle..");

      my %pkgInfos = getZdbName($filter);
      my $lType;

      if(length($pkgInfos{'type'})){
        if($pkgInfos{'type'} =~ /Allianz-Lizenz/){
          $lType = "AL";
        }
        elsif($pkgInfos{'type'} eq "Nationallizenz"){
          $lType = "NL";
        }
        else{
          $lType = $owner;
        }
        $logger->info("Verarbeite Paket vom Typ ".$pkgInfos{'type'}."");
      }
      else{
        $logger->error("Konnte den Pakettyp nicht identifizieren.");
        return -1;
      }

      if(length($pkgInfos{'name'})){
        $knownSelection{$filter} = {
          sigel => $filter,
          type => $lType,
          name => $pkgInfos{'name'},
          authority => $pkgInfos{'authority'},
          provider => $pkgInfos{'provider'},
          platform => $pkgInfos{'platform'},
          url => $pkgInfos{'mainUrl'},
          scope => $pkgInfos{'scope'},
        };
      }
      else{
        $logger->error("Zurückgeliefertes Paket hat keinen Namen!");
        return -1;
      }
    }
  }
  else{
    %knownSelection = %known;

    $logger->info("Generating JSON for all packages!");
  }

  $packageDir->mkpath( { verbose => 0 } );
  $titleDir->mkpath( { verbose => 0 } );
  $warningDir->mkpath( { verbose => 0 } );
  $orgsDir->mkpath( { verbose => 0 } );

  my $titlesFile = $titleDir->file("titles_".$filter."_".$endpoint.".json");
  my $orgsFile = $orgsDir->file("gnd_orgs_".$endpoint.".json");

  # Warnings

  my %authorityNotes;
  my %authorityNotesZDB;
  my %authorityNotesGVK;

  my $out_warnings;
  my $out_warnings_zdb;
  my $out_warnings_gvk;
  my $wdir;
  my $wfile;
  my $wzfile;
  my $wgfile;

  if($filter){
    $warningDir = $warningDir->subdir($filter);
    $warningDir->mkpath({verbose => 0});

    $orgsDir = $orgsDir->subdir($filter);
    $orgsDir->mkpath({verbose => 0});
  }

  $wfile = $warningDir->file("Warnings_all_$endpoint.json");

  $wfile->touch();
  $out_warnings = $wfile->openw();

  $wzfile = $warningDir->file("Warnings_zdb_all_$endpoint.json");

  $wzfile->touch();
  $out_warnings_zdb = $wzfile->openw();

  $wgfile = $warningDir->file("Warnings_gvk_all_$endpoint.json");

  $wgfile->touch();
  $out_warnings_gvk = $wgfile->openw();


  # Statistics

  my %globalStats;

  # Output file handling

  my $json_warning = JSON->new->utf8->canonical;
  my $json_warning_zdb = JSON->new->utf8->canonical;
  my $json_warning_gvk = JSON->new->utf8->canonical;
  my $json_titles = JSON->new->utf8->canonical;
  my $json_orgs = JSON->new->utf8->canonical;

  # Start timer

  my $startTime = time();
  my $packagesTotal = 0;
  my $skippedPackages = "";
  my $skippedTitles = 0;
  my $noTipps = "";

  foreach my $sigel (keys %knownSelection){

    $logger->info("Processing Package ".($packagesTotal + 1).", ".$sigel."...");

    my $pkgScope = $knownSelection{$sigel}{'scope'};
    my $noZdbOrgs = 0;

    my $packageFile = $packageDir->file($sigel."_".$endpoint.".json");

    if($knownSelection{$sigel}{'zdbOrgs'} && scalar @{$knownSelection{$sigel}{'zdbOrgs'}} == 0){
      $noZdbOrgs = 1;
    }

    if($requestedType eq "journal" && $pkgScope !~ /E-Journals/){
      $logger->info("Paket ist nicht als E-Journal-Paket markiert. Überspringe Paket.");
      next;
    }
    my %package;
    my @packageTitles;

    if($localPkg == 1){
      $logger->debug("Verwende lokale Dateien..");
      my $json_pkg = do {
        open(my $json_fh, '<' , $packageFile)
            or $logger->logdie("Can't open \$packageFile\": $!");
        local $/;
        <$json_fh>
      };

      %package = %{decode_json($json_pkg)}
          or $logger->logdie("Paket-JSON nicht lesbar!");

      if (scalar @{$package{'tipps'}} == 0){
        $logger->warn("Paket $sigel hat keine TIPPs und wird nicht angelegt!");
        $noTipps .= "$sigel ";
        next;
      }

      my $json_titles = do {
        open(my $json_fh, '<' , $titlesFile)
            or $logger->warn("Konnte \$tfileName\" nicht öffnen: $!\n");
        local $/;
        <$json_fh>
      };

      if($json_titles){
        @packageTitles = @{decode_json($json_titles)}
            or $logger->warn("Titel-JSON konnte nicht gelesen werden!\n");
      }

    }
    else{

      my %currentPackageInfo = %{$knownSelection{$sigel}};

      my ($package, $pkgStats, $pkgWarn, $packageTitles) = processPackage(%currentPackageInfo);

      %package = %{$package};
      @packageTitles = @{$packageTitles};

      if (scalar @{$package{'tipps'}} == 0){
        $logger->warn("Paket $sigel hat keine TIPPs und wird nicht angelegt!");
        $noTipps .= "$sigel ";
        next;
      }

      my %pkgStats = %{$pkgStats};
      my %pkgWarn = %{$pkgWarn};


      $authorityNotes{$knownSelection{$sigel}{'authority'}}{$sigel}{'stats'} = \%pkgStats;
      $authorityNotesZDB{$knownSelection{$sigel}{'authority'}}{$sigel}{'stats'} = \%pkgStats;
      $authorityNotesGVK{$knownSelection{$sigel}{'authority'}}{$sigel}{'stats'} = \%pkgStats;

      $authorityNotes{$knownSelection{$sigel}{'authority'}}{$sigel}{'warnings'} = $pkgWarn{'all'};
      $authorityNotesZDB{$knownSelection{$sigel}{'authority'}}{$sigel}{'warnings'} = $pkgWarn{'zdb'};
      $authorityNotesGVK{$knownSelection{$sigel}{'authority'}}{$sigel}{'warnings'} = $pkgWarn{'gvk'};

      foreach my $pkgStat (keys %pkgStats){
        if (!$globalStats{$pkgStat}){
          $globalStats{$pkgStat} = $pkgStats{$pkgStat};
        }
        else{
          $globalStats{$pkgStat} += $pkgStats{$pkgStat};
        }
      }

      my $json_pkg = JSON->new->utf8->canonical;
      my $out_pkg;

      if($filter){
        $packageFile->touch();
        $out_pkg = $packageFile->openw();

        say $out_pkg $json_pkg->pretty(1)->encode( \%package );
      }
    }

    if($postData == 1){
      sleep 2;

      # Submit collected titles to GOKb

      # my $sumTitles = scalar @packageTitles;

      # $logger->info("Submitting $sumTitles titles to GOKb (".$gokbCreds{'base'}.")");

      # my $titlePostResult = postData('crossReferenceTitle', \@packageTitles);

      # if($titlePostResult != 0){
      #   $logger->error("Error uploading title! Errorcode $titlePostResult");
      # }

      # sleep 2;

      $logger->info("Submitting Package $sigel to GOKb (".$gokbCreds{'base'}.")");

      # Submit package file to GOKb

      my $postResult = postData('crossReferencePackage', \%package);

      if($postResult != 0){
        $logger->error("Could not Upload Package $sigel! Errorcode $postResult");

        if ($postResult != 403) {
          $logger->info("Giving it one more try!");
          sleep 10;

          if(postData('crossReferencePackage', \%package) != 0){
            $logger->error("Second try failed as well. Adding to report..");
            $skippedPackages .= $sigel." ";
          }
        }
        else {
          $skippedPackages .= $sigel." ";
        }
      }
    }
    $packagesTotal++;
  } ## End Package

  # Write collected titles & orgs to file
  if($localPkg == 0){

    # Write collected warnings to file

    say $out_warnings
      $json_warning->pretty(1)->encode( \%authorityNotes );
    say $out_warnings_zdb
      $json_warning_zdb->pretty(1)->encode(\%authorityNotesZDB);
    say $out_warnings_gvk
      $json_warning_gvk->pretty(1)->encode(\%authorityNotesGVK);

    if($filter){
      $titlesFile->touch();

      my $out_titles = $titlesFile->openw();

      say $out_titles $json_titles->pretty(1)->encode( \@allTitles );

      close($out_titles);
    }


    my $out_orgs;

    $orgsFile->touch();

    $out_orgs = $orgsFile->openw();

    say $out_orgs $json_orgs->pretty(1)->encode( \%orgsToAdd );

  }
  else{

    my $json_orgs = do {
      open(my $json_fh, '<' , $orgsFile)
          or $logger->warn("Konnte \$ofileName\" nicht öffnen: $!\n");
      local $/;
      <$json_fh>
    };

    if($json_orgs){
      %orgsToAdd = %{decode_json($json_orgs)}
          or $logger->warn("Org-JSON konnte nicht gelesen werden!\n");
    }
  }

  # Submit new Orgs to GOKb

  my $skippedOrgs = 0;
  my $numNewOrgs = scalar keys %orgsToAdd;

  if($postData == 1 && $newOrgs == 1){
    sleep 3;

    $logger->info("Submitting $numNewOrgs Orgs to GOKb (".$gokbCreds{'base'}.")");

    foreach my $org (keys %orgsToAdd){
      my %curOrg = %{ $orgsToAdd{$org} };
      my $postResult = postData('assertOrg', \%curOrg);

      if($postResult != 0){
        $logger->error("Could not upload Org! Errorcode $postResult");

        $skippedOrgs++;
      }
    }
  }

  ## Final statistics

  my $timeElapsed = duration(time() - $startTime);
  my $finishedRun = strftime '%Y-%m-%d %H:%M:%S', localtime;

  $logger->info("**********************");

  $logger->info("Run finished after $timeElapsed");

  foreach my $gStatKey (keys %globalStats){
    $logger->info("$gStatKey: ".$globalStats{$gStatKey});
  }



  if($skippedPackages ne ""){
    $logger->warn("Wegen Fehler beim Upload übersprungene Pakete: $skippedPackages");
  }

  if($noTipps ne ""){
    $logger->warn("Pakete ohne TIPPs: $noTipps");
  }

  if($skippedTitles != 0){
    $logger->warn("Anzahl wegen Fehler beim Upload übersprungene Titel: $skippedTitles");
  }

  if($skippedOrgs != 0){
    $logger->warn("Anzahl wegen Fehler beim Upload übersprungene Orgs: $skippedOrgs");
  }

  $logger->info("**********************");
}


  ################ PACKAGE ################


sub processPackage {
  my %packageInfo = @_;
  my $currentTitle = 0;
  my %allISSN;
  my %inPackageIDs;
  my @packageTitles;
  my %package;
  my %packageWarnings;

  my %pkgStats = (
    'titlesTotal' => 0,
    'duplicateISSNs' => 0,
    'duplicateZDBids' => 0,
    'noISXN' => 0,
    'wrongISSN' => 0,
    'pubFromGnd' => 0,
    'pubFromAuthor' => 0,
    'pubFromCorp' => 0,
    'numNoUrl' => 0,
    'noPublisherMatch' => 0,
    'noPublisher' => 0,
    'correctedAbbrs' => 0,
    'relDatesInD' => 0,
    'usefulRelated' => 0,
    'nonNlRelation' => 0,
    'possibleRelations' => 0,
    'nlURLs' => 0,
    'otherURLs' => 0,
    'brokenURL' => 0,
    'doi' => 0
  );

  ## Package Header

  my $userListVer = "";
  my $listVerDate = "";

  if($verifyTitleList != 0 && $gokbCreds{'username'}){
    $userListVer = $gokbCreds{'username'};
    $listVerDate = convertToTimeStamp(strftime('%Y-%m-%d', localtime));
  }
  my $gokbProvider = matchExistingOrgs($packageInfo{'provider'});

  my $provider = $gokbProvider ? $gokbProvider : $packageInfo{'provider'};
  my $pkgName = $packageInfo{'name'};

  $provider =~ s/Anbieter:\s//;
  $pkgName =~ s/:/ -/g;

  my $pkgYear = strftime '%Y', localtime;

  my %pkgPlatform;
  my $checkedUrl;

  if ( $packageInfo{'url'} ) {

    $checkedUrl = checkUrl($packageInfo{'url'});

    if (!$checkedUrl) {
      my @pkgInfoWarn;

      my %pWarn = (
        'value' => $packageInfo{'url'},
        'status' => 'invalid',
        'message' => 'Die im Sigelverzeichnis für dieses Paket angegebene URL ist nicht erreichbar.'
      );

      push @pkgInfoWarn, \%pWarn;
      $packageWarnings{'all'}{'package'} = \@pkgInfoWarn;
      $packageWarnings{'zdb'}{'package'} = \@pkgInfoWarn;
    }
  }

  if ($packageInfo{'platform'}) {
    if (ref($packageInfo{'platform'}) eq 'HASH') {
      %pkgPlatform = (
        'name' => $packageInfo{'platform'}{'name'},
        'primaryUrl' => $packageInfo{'platform'}{'primaryUrl'}
      );
    }
    else {
      $pkgPlatform{'name'} = $packageInfo{'platform'};
      $pkgPlatform{'primaryUrl'} = $packageInfo{'url'};
    }
  }
  elsif ($checkedUrl) {
    my $pkgUrl = URI->new($packageInfo{'url'});
    my $pkgScheme = $pkgUrl->scheme;
    my $pkgHost = $pkgUrl->host;

    my $urlName = lc (substr($pkgHost, 0, 3) eq 'www' ? substr($pkgHost, 4) : $pkgHost);

    $pkgPlatform{'primaryUrl'} = $packageInfo{'url'};
    $pkgPlatform{'name'} = $urlName;
  }

  my %pkgSource;

  my $pkgType = $packageInfo{'type'};

  $logger->info("Package Type is: $pkgType, endpoint is $endpoint");

  if($endpoint eq 'gvk'){
    %pkgSource = (
      url => "http://sru.gbv.de/gvk",
      name => "GVK-SRU",
      normname => "GVK_SRU"
    );
  }
  elsif($endpoint eq 'zdb'){
    %pkgSource = (
      url => "http://www.zeitschriftendatenbank.de",
      name => "ZDB - Zeitschriftendatenbank"
    );
  }
  elsif($endpoint eq 'natliz'){
    %pkgSource = (
      url => "http://sru.gbv.de/natliz",
      name => "Natliz-SRU",
      normname => "Natliz_SRU"
    );
  }
  elsif($endpoint eq 'fid'){
    %pkgSource = (
      url => "http://sru.gbv.de/fidelio",
      name => "FID-SRU",
      normname => "FID_SRU"
    );
  }
  elsif($endpoint eq 'ebp') {
    %pkgSource = (
      url => "http://sru.k10plus.de/opac-de-627-1",
      name => "EBP-SRU",
      normname => "EBP_SRU"
    );
  }

  my $isConsortial = ($packageInfo{'sigel'} =~ /ZDB-1-/ ? 1 : 0);

  my $pkgNoProv = $pkgName;

  if ($isConsortial != 0) {
    $pkgNoProv = "$pkgName: ".($owner ne "Master" ? $owner : $pkgType);
  }

  my @curGroups = ("VZG");

  if($pkgType eq "NL") {
    push @curGroups, 'NL-DE';
  }
  elsif($pkgType eq "AL") {
    push @curGroups, 'AL-DE';
  }

  $package{'packageHeader'} = {
    name => ($provider ?  "$provider: " : "").$pkgNoProv,
    identifiers => [{ type => "isil", value => $packageInfo{'sigel'} }],
    additionalProperties => [],
    scope => "",
    listStatus => "In Progress",
    editStatus => "In Progress",
    breakable => "No",
    consistent => "Yes",
    fixed => "No",
    paymentType => "",
    global => ($isConsortial ? "Consortium" : "Global"),
    listVerifier => "",
    userListVerifier => $userListVer,
    nominalPlatform => \%pkgPlatform,
    nominalProvider => $provider,
    listVerifiedDate => $listVerDate,
    curatoryGroups => \@curGroups,
  };

  $package{'tipps'} = [];

  my %packageHeader = %{$package{'packageHeader'}};
  my @toQuery = ();
  my %attrsScopes;


  $attrsScopes{'gvk'}{'book'} = {
    query => 'pica.xpr='.$packageInfo{'sigel'}.' and pica.mak=Oa*',
    base => 'http://sru.gbv.de/gvk',
    recordSchema => 'picatitle',
    parser => 'picaxml',
    _max_results => 3
  };

  $attrsScopes{'gvk'}{'journal'} = {
    query => 'pica.xpr='.$packageInfo{'sigel'}.' and (pica.mak=Ob* or pica.mak=Od*)',
    base => 'http://sru.gbv.de/gvk',
    recordSchema => 'picatitle',
    parser => 'picaxml',
    _max_results => 3
  };

  $attrsScopes{'gvk'}{'all'} = {
    query => 'pica.xpr='.$packageInfo{'sigel'}.' and pica.mak=O*',
    base => 'http://sru.gbv.de/gvk',
    recordSchema => 'picatitle',
    parser => 'picaxml',
    _max_results => 3
  };

  $attrsScopes{'gbvcat'}{'book'} = {
    query => 'pica.xpr='.$packageInfo{'sigel'}.' and pica.mak=Oa*',
    base => 'http://sru.gbv.de/gbvcat',
    recordSchema => 'picatitle',
    parser => 'picaxml',
    _max_results => 3
  };

  $attrsScopes{'gbvcat'}{'journal'} = {
    query => 'pica.xpr='.$packageInfo{'sigel'}.' and (pica.mak=Ob* or pica.mak=Od*)',
    base => 'http://sru.gbv.de/gbvcat',
    recordSchema => 'picatitle',
    parser => 'picaxml',
    _max_results => 3
  };

  $attrsScopes{'gbvcat'}{'all'} = {
    query => 'pica.xpr='.$packageInfo{'sigel'}.'  and pica.mak=O*',
    base => 'http://sru.gbv.de/gbvcat',
    recordSchema => 'picatitle',
    parser => 'picaxml',
    _max_results => 3
  };

  $attrsScopes{'zdb'}{'journal'} = {
    query => 'dnb.psg='.$packageInfo{'sigel'}.'  and dnb.frm=O',
    base => 'http://services.dnb.de/sru/zdb',
    recordSchema => 'PicaPlus-xml',
    parser => 'ppxml',
    _max_results => 3
  };

  $attrsScopes{'zdb'}{'all'} = {
    query => 'dnb.psg='.$packageInfo{'sigel'}.'  and dnb.frm=O',
    base => 'http://services.dnb.de/sru/zdb',
    recordSchema => 'PicaPlus-xml',
    parser => 'ppxml',
    _max_results => 3
  };

  $attrsScopes{'natliz'}{'book'} = {
    query => 'pica.xpr='.$packageInfo{'sigel'}.' and pica.mak=Oa*',
    base => 'http://sru.gbv.de/natliz',
    recordSchema => 'picatitle',
    parser => 'picaxml',
    _max_results => 3
  };

  $attrsScopes{'natliz'}{'journal'} = {
    query => 'pica.xpr='.$packageInfo{'sigel'}.' and (pica.mak=Ob* or pica.mak=Od*)',
    base => 'http://sru.gbv.de/natlizzss',
    recordSchema => 'picatitle',
    parser => 'picaxml',
    _max_results => 3
  };

  $attrsScopes{'natliz'}{'database'} = {
    query => 'pica.xpr='.$packageInfo{'sigel'}.' and (pica.mak=Ob* or pica.mak=Od*)',
    base => 'http://sru.gbv.de/natlizfak',
    recordSchema => 'picatitle',
    parser => 'picaxml',
    _max_results => 3
  };

  $attrsScopes{'fid'}{'all'} = {
    query => 'pica.xpr='.$packageInfo{'sigel'}.' and pica.mak=O*',
    base => 'http://sru.gbv.de/fidelio',
    recordSchema => 'picaxml',
    parser => 'picaxml',
    _max_results => 3
  };

  $attrsScopes{'fid'}{'journal'} = {
    query => 'pica.xpr='.$packageInfo{'sigel'}.' and (pica.mak=Ob* or pica.mak=Od*)',
    base => 'http://sru.gbv.de/fidelio',
    recordSchema => 'picaxml',
    parser => 'picaxml',
    _max_results => 3
  };

  $attrsScopes{'fid'}{'book'} = {
    query => 'pica.xpr='.$packageInfo{'sigel'}.' and pica.mak=Oa*',
    base => 'http://sru.gbv.de/fidelio',
    recordSchema => 'picaxml',
    parser => 'picaxml',
    _max_results => 3
  };

  $attrsScopes{'ebp'}{'all'} = {
    query => 'pica.xpr='.$packageInfo{'sigel'},
    base => 'http://sru.k10plus.de/opac-de-627-1',
    recordSchema => 'picaxml',
    parser => 'picaxml'
  };

  $attrsScopes{'ebp'}{'book'} = {
    query => 'pica.xpr='.$packageInfo{'sigel'},
    base => 'http://sru.k10plus.de/opac-de-627-1',
    recordSchema => 'picaxml',
    parser => 'picaxml'
  };

  if ($endpoint eq "natliz" && $requestedType eq 'all') {
    push @toQuery, $attrsScopes{$endpoint}{'book'};
    push @toQuery, $attrsScopes{$endpoint}{'journal'};
    push @toQuery, $attrsScopes{$endpoint}{'database'};
  }
  else {
    push @toQuery, $attrsScopes{$endpoint}{$requestedType};
  }

  foreach my $attrs (@toQuery) {

    my %attrs = %{$attrs};
    my $sruTitles = Catmandu::Importer::SRU->new(%attrs)
      or $logger->logdie("Abfrage über ".$attrs{'base'}." fehlgeschlagen!");

    eval{
      while (my $titleRecord = $sruTitles->next){
        $currentTitle++;
        if(pica_value($titleRecord, '006Z0')){
          $logger->info("Verarbeite Titel ".($currentTitle)." von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '006Z0').")");
        }
        else{
          $logger->info("Verarbeite Titel ".($currentTitle)." von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '003@0').")");
        }

        my ($tipps, $titleStats, $titleWarnings, $titleObj) = processTitle($titleRecord, $pkgType, $endpoint, %packageHeader);

        my @tipps = @{$tipps};
        my %titleStats = %{$titleStats};
        my %titleWarnings = %{$titleWarnings};
        my %titleObj = %{$titleObj};

        push @packageTitles, \%titleObj;

        if(scalar @tipps > 0){
          push @{ $package{'tipps'} }, @tipps;
        }

        foreach my $statsKey (keys %pkgStats){
          if($titleStats{$statsKey}){
            $pkgStats{$statsKey} += $titleStats{$statsKey};
          }
        }

        if(%titleWarnings && $titleWarnings{'id'} ne ""){
          foreach my $wKey (keys %titleWarnings){
            if($wKey ne 'id' && scalar @{$titleWarnings{$wKey}} > 0){
              $packageWarnings{$wKey}{$titleWarnings{'id'}} = $titleWarnings{$wKey};
            }
          }
        }
      }
      1;
    } or do {
      $logger->error("SRU error for ".$packageInfo{'sigel'}.":");
      say $@;

      $package{'tipps'} = [];
    };
  }

  return \%package, \%pkgStats, \%packageWarnings, \@packageTitles;
  $logger->info("Finished processing $currentTitle Titles of package ".$packageInfo{'sigel'});
}

    ################ TITLEINSTANCE ################

sub processTitle {
  my ($titleRecord, $pkgType, $activeSource, %pkgInfo) = @_;
  my $materialType = pica_value($titleRecord, '002@0');
  my $pkgFlag = pica_value($titleRecord, '013H0');
  my $typeChar = substr($materialType, 1, 1);
  my $isJournal = any {$_ eq $typeChar} ("b", "d");
  my $gokbType;
  my $gokbMedium;
  my $ppn = pica_value($titleRecord, '003@0');
  my %titleInfo;
  my @tipps = ();
  my $id;
  my @eissn = ();
  my @pissn = ();
  my @relatedPrev = ();
  my %titleStats;
  my $altType = pica_value($titleRecord, '013Da');

  my %titleWarnings = (
    'id' => "",
    'all' => [],
    'gvk' => [],
    'zdb' => []
  );

    # Check for package flag

  if($pkgFlag && $pkgFlag eq 'pt') {
    $logger->debug("Überspringe Paketaufnahme $ppn...");
    return \@tipps, \%titleStats, \%titleWarnings, \%titleInfo;
  }

    # Process material code

  if($isJournal && $altType ne 'Datenbank'){
    $gokbType = "Serial";
    $gokbMedium = "Journal";
  }
  elsif($altType && $altType eq 'Datenbank'){
    $gokbType = "Database";
    $gokbMedium = "Database";
  }
  elsif($typeChar eq 'a'){
    $gokbType = "Monograph";
    $gokbMedium = "Book";
  }
  else{
    $logger->debug("Überspringe ppn $ppn mit Materialcode '$typeChar'...");
    return \@tipps, \%titleStats, \%titleWarnings, \%titleInfo;
  }

    # -------------------- Identifiers --------------------

  $titleInfo{'identifiers'} = [];

  ## PPN

  if ($activeSource eq "ebp") {
    $titleInfo{'ebp'} = $ppn;
  }

  ## DOI

  my $doi;

  if($activeSource eq "gvk" || $activeSource eq "natliz" || $activeSource eq "gbvcat" || $activeSource eq "ebp"){
    $doi = pica_value($titleRecord, '004V0');
  }
  else{
    $doi = pica_value($titleRecord, '004P0');
  }

  if($doi){
    push @{ $titleInfo{'identifiers'} } , {
      'type' => "doi",
      'value' => $doi
    };

    if($titleStats{'doi'}){
      $titleStats{'doi'}++;
    }
    else{
      $titleStats{'doi'} = 1;
    }
  }

  ## Journal-IDs

  if($isJournal){

    ## ZDB-ID

    if(pica_value($titleRecord, '006Z0')){
      my @zdbIDs = pica_values($titleRecord, '006Z0');

      foreach my $zdbID (@zdbIDs){
        if(formatZdbId($zdbID)){
          $id = formatZdbId($zdbID);
        }
      }

      if($id){
        push @{ $titleInfo{'identifiers'} } , {
          'type' => "zdb",
          'value' => $id
        };
      }
      else{
        $id = $ppn;
        $logger->warn("Konnte ZDB-ID in Titel $ppn nicht validieren!");
      }
    }
    else{
      $logger->warn("Titel mit ppn $ppn hat keine ZDB-ID! Überspringe Titel..");

      push @{ $titleWarnings{'all'} }, {
          '006Z0' => 'Keine ZDB-ID angegeben!'
      };

      push @{ $titleWarnings{'gvk'} }, {
          '006Z0' => 'Keine ZDB-ID angegeben!'
      };
      $id = $ppn;

      return \@tipps, \%titleStats, \%titleWarnings, \%titleInfo;
    }

    ## eISSN

    if(pica_value($titleRecord, '005A0')){

      my @eissnValues = @{pica_fields($titleRecord, '005A')};

      foreach my $eissnValue (@eissnValues) {
        my @eissnValue = @{ $eissnValue };
        my $subfPos = 0;

        foreach my $subField (@eissnValue){
          if($subField eq '0'){
            my $eissn = formatISSN($eissnValue[$subfPos+1]);
            my @globalIssns;

            if($globalIDs{$id}){
              @globalIssns = @{ $globalIDs{$id}{'eissn'} };
            }

            if($eissn eq ""){
              print "ISSN ".pica_value($titleRecord, '005A0');
              print " in Titel $id scheint ungültig zu sein!\n";

              push @{ $titleWarnings{'all'} }, {
                '005A0' => $eissnValue[$subfPos+1],
                'comment' => 'ISSN konnte nicht validiert werden.'
              };

              push @{ $titleWarnings{'zdb'} }, {
                '005A0' => $eissnValue[$subfPos+1],
                'comment' => 'ISSN konnte nicht validiert werden.'
              };
            }
            elsif($globalIDs{$id} && none {$_ eq $eissn} @globalIssns){
              $logger->warn("eISSN $eissn kommt in bereits erschlossenem Titel $id nicht vor!");

              push @{ $titleWarnings{'all'} }, {
                '005A0' => $eissn,
                'comment' => 'ISSN bei gleicher ZDB-ID nicht vergeben?'
              };

              push @{ $titleWarnings{'zdb'} }, {
                '005A0' => $eissn,
                'comment' => 'ISSN bei gleicher ZDB-ID nicht vergeben?'
              };
            }
            else{
              push @eissn, $eissn;

              push @{ $titleInfo{'identifiers'}} , {
                'type' => "eissn",
                'value' => $eissn
              };


            }
          }
          $subfPos++;
        }
      }
    }
    else{
      if($titleStats{'noISXN'}){
        $titleStats{'noISXN'}++;
      }
      else{
        $titleStats{'noISXN'} = 1;
      }
    }

    ## pISSN

    if(pica_value($titleRecord, '005P0')){
      my @issnValues = @{pica_fields($titleRecord, '005P')};

      foreach my $issnValue (@issnValues) {
        my @issnValue = @{ $issnValue };
        my $subfPos = 0;

        foreach my $subField (@issnValue){
          if($subField eq '0'){
            my $pissn = formatISSN($issnValue[$subfPos+1]);

            if($pissn eq ""){
              print "Parallel-ISSN ".pica_value($titleRecord, '005P0');
              print " in Titel $id scheint ungültig zu sein!\n";

              push @{ $titleWarnings{'all'} }, {
                '005P0' => $issnValue[$subfPos+1]
              };
              push @{ $titleWarnings{'zdb'} }, {
                '005P0' => $issnValue[$subfPos+1]
              };
            }
            else{

              if($allISSN{$pissn}){
                print "Parallel-ISSN $pissn";
                print " in Titel $id wurde bereits als eISSN vergeben!\n";

                if($titleStats{'wrongISSN'}){
                  $titleStats{'wrongISSN'}++;
                }
                else{
                  $titleStats{'wrongISSN'} = 1;
                }

                push @{ $titleWarnings{'all'} }, {
                  '005P0' => $issnValue[$subfPos+1],
                  'comment' => 'gleiche Vorgänger-eISSN als Parallel-ISSN?'
                };
                push @{ $titleWarnings{'zdb'} }, {
                  '005P0' => $issnValue[$subfPos+1],
                  'comment' => 'gleiche Vorgänger-eISSN als Parallel-ISSN?'
                };
              }

              push @pissn, $pissn;

              push @{ $titleInfo{'identifiers'}} , {
                'type' => "issn",
                'value' => $pissn
              };
            }
          }
          $subfPos++;
        }
      }
    }

  }
  elsif($gokbMedium eq "Book"){
    $id = $ppn;
    my $numIsbn = 0;
    my @isbns;

    if(pica_value($titleRecord, '004A')){
      my @isbnValues = @{pica_fields($titleRecord, '004A')};

      foreach my $isbnValue (@isbnValues) {
        my @isbnValue = @{ $isbnValue };
        my $isbn;
        my $isbnType;
        my $subfPos = 0;

        foreach my $subField (@isbnValue) {

          if($subField eq 'A'){
            if(!$isbn){
              $isbn = $isbnValue[$subfPos+1];
              $isbn =~ s/-\s//g;
            }
            else{
              $logger->error("Mehrere ISBNs in einem PICA-Feld!");
            }
          }
          if($subField eq '0'){
            if(!$isbn){
              $isbn = $isbnValue[$subfPos+1];
              $isbn =~ s/-\s//g;
            }
            else{
              $logger->error("Mehrere ISBNs in einem PICA-Feld!");
            }
          }
          if($subField eq 'f' && scalar $isbnValue >= $subfPos+1){
            $isbnType = $isbnValue[$subfPos+1];
          }
          $subfPos++;
        }

        if ($isbn && length($isbn) == 13 && none {$_ == $isbn} @isbns && (!$isbnType || $isbnType eq 'Online' || $isbnType eq 'ebook' || $isbnType eq 'electronic bk.')) {
          push @{ $titleInfo{'identifiers'}} , {
            'type' => "isbn",
            'value' => $isbn
          };
          $numIsbn++;
        }
      }

      if (pica_value($titleRecord, '028Aa')) {
        $titleInfo{'firstAuthor'} = pica_value($titleRecord, '028Aa');
      }
    }

    if($numIsbn == 0) {
      if($titleStats{'noISXN'}){
        $titleStats{'noISXN'}++;
      }
      else{
        $titleStats{'noISXN'} = 1;
      }
    }
  }
  else{
    $logger->debug("Überspringe ppn $ppn mit Materialcode '$typeChar'...");
    return \@tipps, \%titleStats, \%titleWarnings, \%titleInfo;
  }
  $titleWarnings{'id'} = $id;
  ## Andere Identifier, z.B. OCLC-No.



  # Check, if the title is a journal
  # (shouldn't be necessary since it should be included in the search query)

  if(($requestedType eq "journal" && !$isJournal) || ($requestedType eq "book" && $isJournal)){
    $logger->warn("Überspringe Titel ".pica_value($titleRecord, '021Aa').", Materialcode: $materialType");

    return \@tipps, \%titleStats, \%titleWarnings, \%titleInfo;
  }

  # -------------------- Title --------------------

  if(pica_value($titleRecord, '021Ca')){
    my $titleField = pica_value($titleRecord, '021Aa');

    if($titleField =~ /@/){
      $titleField =~ s/@//;
    }
    $titleInfo{'name'} = $titleField;

    if(pica_value($titleRecord, '021Ad') && $typeChar eq 'a'){
      $titleInfo{'name'} .= " - ".pica_value($titleRecord, '021Ad');
    }

    $titleInfo{'name'} .= " - ".pica_value($titleRecord, '021Ca');
  }
  elsif(pica_value($titleRecord, '025@a')){
    my $titleField = pica_value($titleRecord, '025@a');

    if($titleField =~ /@/){
      $titleField =~ s/@//;
      $logger->debug("Removed \@ from Title!");
    }
    $titleInfo{'name'} = $titleField;

  }
  elsif(pica_value($titleRecord, '021Aa')){
    my $titleField = pica_value($titleRecord, '021Aa');

    if($titleField =~ /@/){
      $titleField =~ s/@//;
    }
    $titleInfo{'name'} = $titleField;

    if(pica_value($titleRecord, '021Ad') && $typeChar eq 'a'){
      $titleInfo{'name'} .= " - ".pica_value($titleRecord, '021Ad');
    }

  }
  else{
    $logger->info("Keinen Titel für ".$ppn." erkannt, überspringe Titel!");

    push @{ $titleWarnings{'all'} }, {
        '021Aa' => pica_value($titleRecord, '021Aa'),
        'comment' => "Kein Titel gefunden!"
    };
    push @{ $titleWarnings{'zdb'} }, {
        '021Aa' => pica_value($titleRecord, '021Aa'),
        'comment' => "Kein Titel gefunden!"
    };

    return \@tipps, \%titleStats, \%titleWarnings, \%titleInfo;
  }

  # -------------------- Other GOKb Fields --------------------

  $titleInfo{'type'} = $gokbType;
  $titleInfo{'status'} = "Current";
  $titleInfo{'editStatus'} = "In Progress";
  $titleInfo{'shortcode'} = "";
  $titleInfo{'medium'} = $gokbMedium;
  $titleInfo{'defaultAccessURL'} = "";
  $titleInfo{'OAStatus'} = "";
  $titleInfo{'issuer'} = "";
  $titleInfo{'imprint'} = "";
  $titleInfo{'continuingSeries'} = "";

  # -------------------- Release notes --------------------

  my @releaseNotes = @{ pica_fields($titleRecord, '031N') };

  if(pica_value($titleRecord, '032@a')) {
    $titleInfo{'editionStatement'} = pica_value($titleRecord, '032@a');
  }

  if(pica_value($titleRecord, '036El')) {
    my ($volNumber) = pica_value($titleRecord, '036El') =~ /(\d+)/;

    if($volNumber) {
      $titleInfo{'volumeNumber'} = $volNumber;
    }
  }

  my %releaseStart = (
    'year' => "",
    'month' => "",
    'day' => "",
    'volume' => "",
    'issue' => ""
  );
  my %releaseEnd = (
    'year' => "",
    'month' => "",
    'day' => "",
    'volume' => "",
    'issue' => ""
  );

  foreach my $releaseNote (@releaseNotes) {
    my @releaseNote = @{ $releaseNote };
    my $subfPos = 0;
    my @subfArray = ("j","c","b","d","e","k","m","l","n","o");

    foreach my $subField (@releaseNote){
      if(any {$_ eq $subField} @subfArray){
        if($releaseNote[$subfPos+1] =~ /\D/){
          my $fullField = "031N$subField";

          push @{ $titleWarnings{'all'} }, {
              $fullField => $releaseNote[$subfPos+1],
              'comment' => "Nicht-Zahlenwerte in Zahlenfeld"
          };
          push @{ $titleWarnings{'zdb'} }, {
              $fullField => $releaseNote[$subfPos+1],
              'comment' => "Nicht-Zahlenwerte in Zahlenfeld"
          };
        }
      }
      if($subField eq 'j'){
        if($releaseStart{'year'} ne ""){
          $releaseStart{'year'} = substr($releaseNote[$subfPos+1],0,4);
        }
        else{
          if($releaseEnd{'year'} ne ""){
            $releaseEnd{'year'} = "";
            $releaseEnd{'month'} = "";
            $releaseEnd{'day'} = "";
            $releaseEnd{'volume'} = "";
            $releaseEnd{'issue'} = "";
          }
        }
      }
      elsif($subField eq 'c' && $releaseStart{'month'} ne ""){
        $releaseStart{'month'} = $releaseNote[$subfPos+1];
      }
      elsif($subField eq 'b' && $releaseStart{'day'} ne ""){
        $releaseStart{'day'} = $releaseNote[$subfPos+1];
      }
      elsif($subField eq 'd' && $releaseStart{'volume'} ne ""){
        $releaseStart{'volume'} = $releaseNote[$subfPos+1];
      }
      elsif($subField eq 'e' && $releaseStart{'issue'} ne ""){
        $releaseStart{'issue'} = $releaseNote[$subfPos+1];
      }
      elsif($subField eq 'k'){
        $releaseEnd{'year'} = substr($releaseNote[$subfPos+1],0,4);
      }
      elsif($subField eq 'm' && !$releaseEnd{'month'}){
        $releaseEnd{'month'} = $releaseNote[$subfPos+1];
      }
      elsif($subField eq 'l' && !$releaseEnd{'day'}){
        $releaseEnd{'day'} = $releaseNote[$subfPos+1];
      }
      elsif($subField eq 'n' && !$releaseEnd{'volume'}){
        $releaseEnd{'volume'} = $releaseNote[$subfPos+1];
      }
      elsif($subField eq 'o' && !$releaseEnd{'issue'}){
        $releaseEnd{'issue'} = $releaseNote[$subfPos+1];
      }

      $subfPos++;
    }
  }

  # -------------------- Publication Dates --------------------

  my $start_year = 0;
  my $start_month = 0;
  my $start_day = 0;
  my $end_year = 0;
  my $end_month = 0;
  my $end_day = 0;

  if(pica_value($titleRecord, '011@a')){
    $start_year = pica_value($titleRecord, '011@a');
  }
  elsif($releaseStart{'year'} ne ""){
    $start_year = $releaseStart{'year'};
  }

  if(pica_value($titleRecord, '011@b')){
    if($start_year != 0
      && pica_value($titleRecord, '011@b') >= $start_year
    ){
      $end_year = pica_value($titleRecord, '011@b');
    }
  }
  elsif($releaseEnd{'year'} ne ""){
    if($start_year != 0 && $releaseEnd{'year'} >= $start_year ){
      $end_year = $releaseEnd{'year'};
    }
  }

  if(pica_value($titleRecord, '011@a')
    && $releaseStart{'year'} eq pica_value($titleRecord, '011@a')
  ){
    if(looks_like_number($releaseStart{'month'})){
      $start_month = $releaseStart{'month'};
    }

    if(looks_like_number($releaseStart{'day'}) && $start_month != 0){
      $start_day = $releaseStart{'day'};
    }
  }
  if(pica_value($titleRecord, '011@b')
    && $releaseEnd{'year'} eq pica_value($titleRecord, '011@b')
  ){
    if(looks_like_number($releaseEnd{'month'})){
      $end_month = $releaseEnd{'month'};
    }

    if(looks_like_number($releaseEnd{'day'}) && $end_month != 0){
      $end_day = $releaseEnd{'day'};
    }
  }

  my %dates = (
    'startj' => $start_year,
    'startm' => $start_month,
    'startd' => $start_day,
    'endj' => $end_year,
    'endm' => $end_month,
    'endd' => $end_day
  );

  my @dts = transformDate(\%dates);
  if($isJournal){
    $titleInfo{'publishedFrom'} = convertToTimeStamp($dts[0][0], 0);

    $titleInfo{'publishedTo'} = convertToTimeStamp($dts[0][1], 1);
  }
  else{
    $titleInfo{'dateFirstInPrint'} = convertToTimeStamp($dts[0][0], 0);
  }

  # -------------------- Publisher --------------------

  $titleInfo{'publisher_history'} = [];

  my @possiblePubs = @{ pica_fields($titleRecord, '033A') };
  my $checkPubs = pica_value($titleRecord, '033An');
  my @altPubs = @{ pica_fields($titleRecord, '033B') };

  push(@possiblePubs, @altPubs);
  my @gndPubs;

  if($activeSource eq "gvk" || $activeSource eq "gbvcat"){
    @gndPubs = @{ pica_fields($titleRecord, '029G') };
  }
  elsif($activeSource eq "zdb"){
    @gndPubs = @{ pica_fields($titleRecord, '029A') };
  }

  my $authorField = pica_value($titleRecord, '021Ah');
  my $titleCorpField = pica_value($titleRecord, '021Ae');
  my $corpField = pica_value($titleRecord, '029Aa');

  if(!$checkPubs){
    if($titleStats{'noPublisher'}){
      $titleStats{'noPublisher'}++;
    }
    else{
      $titleStats{'noPublisher'} = 1;
    }
  }

  if(scalar @possiblePubs > 0){
    foreach my $pub (@possiblePubs) {
      my @pub = @{ $pub };
      my $tempPub;
      my $pubStart;
      my $pubEnd;
      my $subfPos = 0;
      my $pubStatus = 'Active';
      my $preCorrectedPub = "";

      foreach my $subField (@pub){

        if($subField eq 'n'){
          if($tempPub){
            push @{ $titleWarnings{'all'} }, {
              '033(A/B)' => \@pub,
              'comment' => "Mehrere Verlage in einem PICA-Feld!"
            };
            if($activeSource eq "gvk" || $activeSource eq "gbvcat"){
              push @{ $titleWarnings{'gvk'} }, {
                '033(A/B)' => \@pub,
                'comment' => "Mehrere Verlage in einem PICA-Feld!"
              };
            }
            elsif($activeSource eq "zdb"){
              push @{ $titleWarnings{'zdb'} }, {
                '033(A/B)' => \@pub,
                'comment' => "Mehrere Verlage in einem PICA-Feld!"
              };
            }
          }
          else {
            $preCorrectedPub = $pub[$subfPos+1];
            $tempPub = $pub[$subfPos+1];
            $logger->debug("Verlagsangabe: $tempPub");
          }
        }
        if($subField eq 'h'){

          if( $pub[$subfPos+1] =~ /[a-zA-Z\.,\(\)]+/ ) {
            push @{ $titleWarnings{'all'} }, {
              '033(A/B)h' => $pub[$subfPos+1],
              'comment' => "Keine reine Jahresangabe für Verlag"
            };
            push @{ $titleWarnings{'zdb'} }, {
              '033(A/B)h' => $pub[$subfPos+1],
              'comment' => "Keine reine Jahresangabe für Verlag"
            };
            if($pub[$subfPos+1] eq 'früher' || $pub[$subfPos+1] eq 'anfangs'){
              $pubStatus = 'Previous'
            }
          }
          my ($tempStart) =
            $pub[$subfPos+1] =~ /([0-9]{4})\/?[0-9]{0,2}\s?-/;

          if($tempStart && looks_like_number($tempStart)) {
            $pubStart = convertToTimeStamp($tempStart, 0);
          }
          elsif($titleInfo{'publishedFrom'}){
            $pubStart = $titleInfo{'publishedFrom'}
          }
          elsif($titleInfo{'dateFirstOnline'}) {
            $pubStart = $titleInfo{'dateFirstOnline'}
          }

          my ($tempEnd) =
            $pub[$subfPos+1] =~ /-\s?([0-9]{4})/;

          if($tempEnd && looks_like_number($tempEnd)) {
            $pubEnd = convertToTimeStamp($tempEnd, 1);
            $pubStatus = 'Previous'
          }
        }
        $subfPos++;
      }

      if(!$tempPub){
        next;
      }

      ## RAK-Abkürzungen ersetzen/auflösen

      $tempPub =~ s/[\[\]]//g;
      $tempPub =~ s/u\.\s?a\.//g;
      $tempPub =~ s/\s\@//g;

      if($tempPub =~ /(^|\s)[pP]ubl\.?(\s|$)/){

        $tempPub =~ s/(^|\s)([pP]ubl)\.?(\s|$)/$1Pub$3/g;

        if($titleStats{'correctedAbbrs'}){
          $titleStats{'correctedAbbrs'}++;
        }
        else{
          $titleStats{'correctedAbbrs'} = 1;
        }
      }

      if($tempPub =~ /(^|\s)[Aa]ssoc\.?(\s|$)/){

        $tempPub =~ s/(^|\s)([Aa]ssoc)\.?(\s|$)/$1Association$3/g;
        if($titleStats{'correctedAbbrs'}){
          $titleStats{'correctedAbbrs'}++;
        }
        else{
          $titleStats{'correctedAbbrs'} = 1;
        }
      }

      if($tempPub =~ /(^|\s)[Ss]oc\.?(\s|$)/){

        $tempPub =~ s/(^|\s)([Ss]oc)\.?(\s|$)/$1Society$3/g;
        if($titleStats{'correctedAbbrs'}){
          $titleStats{'correctedAbbrs'}++;
        }
        else{
          $titleStats{'correctedAbbrs'} = 1;
        }
      }

      if($tempPub =~ /(^|\s)[Uu]niv\.?(\s|$)/){

        $tempPub =~ s/(^|\s)([Uu]niv)\.?(\s|$)/$1University$3/g;
        if($titleStats{'correctedAbbrs'}){
          $titleStats{'correctedAbbrs'}++;
        }
        else{
          $titleStats{'correctedAbbrs'} = 1;
        }
      }

      if($tempPub =~ /(^|\s)[Aa]cad\.?(\s$)/){

        $tempPub =~ s/(^|\s)([Aa]cad)\.?(\s|$)/$1Academic$3/g;
        if($titleStats{'correctedAbbrs'}){
          $titleStats{'correctedAbbrs'}++;
        }
        else{
          $titleStats{'correctedAbbrs'} = 1;
        }
      }

      if($tempPub =~ /(^|\s)[Vv]erl\.?(\s$)/){

        $tempPub =~ s/(^|\s)([Vv]erl)\.?(\s|$)/$1Verlag$3/g;
        if($titleStats{'correctedAbbrs'}){
          $titleStats{'correctedAbbrs'}++;
        }
        else{
          $titleStats{'correctedAbbrs'} = 1;
        }
      }

      if($tempPub =~ /(^|\s)[Aa]kad\.?(\s$)/){

        $tempPub =~ s/(^|\s)([Aa]kad)\.?(\s|$)/$1Akademie$3/g;
        if($titleStats{'correctedAbbrs'}){
          $titleStats{'correctedAbbrs'}++;
        }
        else{
          $titleStats{'correctedAbbrs'} = 1;
        }
      }

      if($tempPub =~ /(^|\s)[Vv]erb\.?(\s$)/){

        $tempPub =~ s/(^|\s)([Vv]erb)\.?(\s|$)/$1Verband$3/g;
        if($titleStats{'correctedAbbrs'}){
          $titleStats{'correctedAbbrs'}++;
        }
        else{
          $titleStats{'correctedAbbrs'} = 1;
        }
      }

      ## Verlag verifizieren & hinzufügen

      my $ncsuPub = matchExistingOrgs($tempPub);

      if($ncsuPub){
        push @{ $titleInfo{'publisher_history'}} , {
            'name' => $ncsuPub,
            'startDate' => $pubStart ? $pubStart : "",
            'endDate' => $pubEnd ? $pubEnd : "",
            'status' => "Active"
        };
      }
      elsif(!$ncsuPub
        || $tempPub =~ /[\[\]]/
        || $tempPub =~ /u\.\s?a\./
      ){
        if($titleStats{'noPublisherMatch'}){
          $titleStats{'noPublisherMatch'}++;
        }
        else{
          $titleStats{'noPublisherMatch'} = 1;
        }

        push @{ $titleWarnings{'all'} }, {
          '033(A/B)n' => $preCorrectedPub,
          'comment' => "Verlagsname konnte nicht verifiziert werden."
        };
        push @{ $titleWarnings{'zdb'} }, {
          '033(A/B)n' => $preCorrectedPub,
          'comment' => "Verlagsname konnte nicht verifiziert werden."
        };
      }
    }
  }

  if(scalar @gndPubs > 0){
    foreach my $pub (@gndPubs) {
      my @pub = @{ $pub };
      my $tempPub;
      my $pubStart;
      my $pubEnd;
      my $subfPos = 0;
      my $preCorrectedPub = "";
      my $pubName;
      my $isParent = 0;
      my $branch;
      my $ncsuPub;
      my $authType;
      my $gndID;

      foreach my $subField (@pub){
        if($subField eq 'a'){
          $pubName = $pub[$subfPos+1];
          $pubName =~ s/\s@//g;
          $ncsuPub = matchExistingOrgs($pubName);

          if(!$ncsuPub){
            if($endpoint eq 'zdb'){
              push @{ $titleWarnings{'all'} }, {
                '029Aa' => $pubName,
                'comment' => "GND-Org ist nicht in der GOKb vorhanden."
              };
            }
            else{
              push @{ $titleWarnings{'all'} }, {
                '029Ga' => $pubName,
                'comment' => "GND-Org ist nicht in der GOKb vorhanden."
              };
            }
          }
        }
        elsif($subField eq 'M' || $subField eq '7'){
          $authType = substr($pub[$subfPos+1],0,2);
        }
        elsif($subField eq '0'){
          $gndID = $pub[$subfPos+1];
        }
        elsif($subField eq 'b'){
          $branch = $pub[$subfPos+1];
          $isParent = 1;
        }
        $subfPos++;
      }
      if($isParent == 1){
        $logger->debug("$id - Parent: $pubName, Child: $branch");

        if($activeSource eq 'zdb'){
          push @{ $titleWarnings{'all'} }, {
            '029A' => \@pub,
            'comment' => "GND-Org ist nicht eigenständig."
          };
        }
        else{
          push @{ $titleWarnings{'all'} }, {
            '029G' => \@pub,
            'comment' => "GND-Org ist nicht eigenständig."
          };
        }
        next;
      }

      if($authType && $authType =~ /Tb/ && $gndID){

        my $orgURI = "http://d-nb.info/gnd/".$gndID;

        my %orgObj = (
          'name' => $pubName,
          'identifiers' => [{'type' => "global", 'value' => $orgURI}]
        );

        if(!$orgsToAdd{$pubName}){
          $orgsToAdd{$pubName} = \%orgObj;
        }

        if(scalar @{ $titleInfo{'publisher_history'} } == 0) {

          push @{ $titleInfo{'publisher_history'}} , {
              'name' => $pubName,
              'startDate' => "",
              'endDate' => "",
              'status' => "Active"
          };

          if($titleStats{'pubFromGnd'}){
            $titleStats{'pubFromGnd'}++;
          }
          else{
            $titleStats{'pubFromGnd'} = 1;
          }
        }
      }
    }
  }

  ## Im Autor- bzw. Körperschaftsfeld nach Ersatz suchen

  if(scalar @{ $titleInfo{'publisher_history'} } == 0) {
    if($titleCorpField){
      my $ncsuAuthor = matchExistingOrgs($titleCorpField);

      if($ncsuAuthor){
        push @{ $titleInfo{'publisher_history'}} , {
            'name' => $ncsuAuthor,
            'startDate' => convertToTimeStamp($dts[0][0], 0),
            'endDate' => convertToTimeStamp($dts[0][1], 1),
            'status' => ""
        };

        if($titleStats{'pubFromCorp'}){
          $titleStats{'pubFromCorp'}++;
        }
        else{
          $titleStats{'pubFromCorp'} = 1;
        }
      }
      # print "Used author $authorField as publisher.\n";
    }
    elsif($authorField){
      my $ncsuAuthor = matchExistingOrgs($authorField);

      if($ncsuAuthor){
        push @{ $titleInfo{'publisher_history'}} , {
            'name' => $ncsuAuthor,
            'startDate' => convertToTimeStamp($dts[0][0], 0),
            'endDate' => convertToTimeStamp($dts[0][1], 1),
            'status' => ""
        };

        if($titleStats{'pubFromAuthor'}){
          $titleStats{'pubFromAuthor'}++;
        }
        else{
          $titleStats{'pubFromAuthor'} = 1;
        }
      }
      # print "Used author $authorField as publisher.\n";
    }
    elsif($corpField){
      my $ncsuCorp = matchExistingOrgs($corpField);

      if($ncsuCorp){
        push @{ $titleInfo{'publisher_history'}} , {
            'name' => $ncsuCorp,
            'startDate' => convertToTimeStamp($dts[0][0], 0),
            'endDate' => convertToTimeStamp($dts[0][1], 1),
            'status' => ""
        };
        if($titleStats{'pubFromCorp'}){
          $titleStats{'pubFromCorp'}++;
        }
        else{
          $titleStats{'pubFromCorp'} = 1;
        }
      }
    }
      # print "Used corp $corpField as publisher.\n";
  }

  # -------------------- Related titles --------------------

  my @relatedTitles = @{ pica_fields($titleRecord, '039E') };

  foreach my $relatedTitle (@relatedTitles){
    my @relTitle = @{ $relatedTitle };
    my $relationType;
    my $altRelationType;
    my $relName;
    my $relatedID;
    my @connectedIDs;
    my $relPPN;
    my $relatedDates;
    my $relIsNl = 0;
    my $isDirectRelation = 0;
    my $rStartYear;
    my $rEndYear;
    my $subfPos = 0;
    my %relObj = (
      'title' => "",
      'identifiers' => []
    );

    foreach my $subField (@relTitle){
      if($activeSource eq "gvk" || $activeSource eq "gbvcat"){
        if($subField eq 'c'){

          $altRelationType = $relTitle[$subfPos+1];

        }
        elsif($subField eq 'b'){

          $relationType = $relTitle[$subfPos+1];

        }
        elsif($subField eq 'ZDB' && $relTitle[$subfPos+1] eq '6'){
          my $oID = formatZdbId($relTitle[$subfPos+2]);

          if($oID){
            $relatedID = $oID;
          }
        }
        elsif($subField eq 't' || $subField eq 'a'){

          $relName = $relTitle[$subfPos+1];

        }
        elsif($subField eq 'f' || $subField eq 'd'){
          my $cleanedRelDates = $relTitle[$subfPos+1] =~ s/\[\]//g;
          my ($tempStartYear) = $cleanedRelDates =~ /([0-9]{4})\s?-/;
          my ($tempEndYear) = $cleanedRelDates =~ /-\s?([0-9]{4})[^\.]?/;

          if($tempEndYear){
            $rEndYear = $tempEndYear;

            if($subField eq 'd') {
              push @{ $titleWarnings{'all'} }, {
                '039Ed' => $relTitle[$subfPos+1],
                'comment' => 'Datumsangaben gehören in Unterfeld f.'
              };
              push @{ $titleWarnings{'gvk'} }, {
                '039Ed' => $relTitle[$subfPos+1],
                'comment' => 'Datumsangaben gehören in Unterfeld f.'
              };
              if($titleStats{'relDatesInD'}){
                $titleStats{'relDatesInD'}++;
              }
              else{
                $titleStats{'relDatesInD'} = 1;
              }
            }
          }
          if($tempStartYear){
            $rStartYear = $tempStartYear;

            if($subField eq 'd') {
              push @{ $titleWarnings{'all'} }, {
                '039Ed' => $relTitle[$subfPos+1],
                'comment' => 'Datumsangaben gehören in Unterfeld f.'
              };
              push @{ $titleWarnings{'gvk'} }, {
                '039Ed' => $relTitle[$subfPos+1],
                'comment' => 'Datumsangaben gehören in Unterfeld f.'
              };

              if($titleStats{'relDatesInD'}){
                $titleStats{'relDatesInD'}++;
              }
              else{
                $titleStats{'relDatesInD'} = 1;
              }
            }
          }
          if($subField eq 'f'){
            $relatedDates = $relTitle[$subfPos+1];

            unless($relatedDates =~ /[0-9]{4}\s?-\s?[0-9]{0,4}/){
              push @{ $titleWarnings{'all'} }, {
                '039Ef' => $relTitle[$subfPos+1],
                'comment' => 'Keine gültige Datumsangabe.'
              };
              push @{ $titleWarnings{'gvk'} }, {
                '039Ef' => $relTitle[$subfPos+1],
                'comment' => 'Keine gültige Datumsangabe.'
              };
            }
          }
        }
      }
      elsif($activeSource eq "zdb"){
        if($subField eq 'b'){
          $relationType = $relTitle[$subfPos+1];
        }

        if($subField eq '0' && $relTitle[$subfPos-1] ne 'gnd'){
          my $oID = formatZdbId($relTitle[$subfPos+1]);

          if($oID){
            $relatedID = $oID;
          }
        }
        if($subField eq 'Y'){
          $relName = $relTitle[$subfPos+1];
        }

        if($subField eq 'D' && $relTitle[$subfPos-1] ne 'N'){
          $relName = $relTitle[$subfPos+1];
        }

        if($subField eq 'H'){
          my $cleanedRelDates = $relTitle[$subfPos+1] =~ s/\[\]//g;
          my $tempStartYear;
          if($cleanedRelDates =~ /^[0-9]{4}$/){
            $tempStartYear = $cleanedRelDates;
          }
          else{
            $tempStartYear = $cleanedRelDates =~ /([0-9]{4})\s?-/;
          }
          my ($tempEndYear) = $cleanedRelDates =~ /-\s?([0-9]{4})[^\.]?/;

          if($tempEndYear){
            $rEndYear = $tempEndYear;
          }
          if($tempStartYear){
            $rStartYear = $tempStartYear;
          }
          $relatedDates = $relTitle[$subfPos+1];

          unless($relatedDates =~ /[0-9]{4}\s?-?\s?[0-9]{0,4}/ || $relatedDates =~ /[a-zA-Z=\/]+/){
            push @{ $titleWarnings{'all'} }, {
              '039EH' => $relTitle[$subfPos+1],
              'comment' => 'Keine gültige Datumsangabe.'
            };
            push @{ $titleWarnings{'zdb'} }, {
              '039EH' => $relTitle[$subfPos+1],
              'comment' => 'Keine gültige Datumsangabe.'
            };
          }
        }
      }
      $subfPos++;
    }
    if($relatedID){
      if($titleStats{'possibleRelations'}){
        $titleStats{'possibleRelations'}++;
      }
      else{
        $titleStats{'possibleRelations'} = 1;
      }

      if(!$relationType && $altRelationType) {
        $relationType = $altRelationType;
      }

      $logger->debug("Found possible relation $relatedID");

      my %relAttrs;

      if($activeSource eq "gvk" || $activeSource eq "gbvcat"){
        my $relQryString = 'pica.zdb='.$relatedID;

        if($requestedType eq "journal"){
          $relQryString .= ' and (pica.mak=Ob* or pica.mak=Od*)';
        }

        %relAttrs = (
          base => 'http://sru.gbv.de/gvk',
          query => $relQryString,
          recordSchema => 'picatitle',
          parser => 'picaxml',
          _max_results => 5
        );

        if($activeSource eq "gbvcat") {
          $relAttrs{'base'} = 'http://sru.gbv.de/gbvcat'
        }

      }
      elsif($activeSource eq "zdb"){
        my $relQryString = 'dnb.zdbid='.$relatedID;

        %relAttrs = (
          base => 'http://services.dnb.de/sru/zdb',
          query => $relQryString,
          recordSchema => 'PicaPlus-xml',
          parser => 'ppxml',
          _max_results => 1
        );
      }
      elsif($activeSource eq "natliz"){
        my $relQryString = 'pica.zdb='.$relatedID;

        %relAttrs = (
          base => 'http://sru.gbv.de/natlizzss',
          query => $relQryString,
          recordSchema => 'picatitle',
          parser => 'picaxml',
          _max_results => 1
        );
      }
      my $relRecord;

      eval{
        my $sruRel = Catmandu::Importer::SRU->new(%relAttrs)
          or $logger->warn("Abfrage über ".$relAttrs{'base'}." fehlgeschlagen!");

        $relRecord = $sruRel->first();
      }; warn $@ if $@;

      if($relRecord && ref($relRecord) eq 'HASH'){
        $relPPN = pica_value($relRecord, '003@0');

        if($activeSource eq "gvk" || $activeSource eq "gbvcat"){
          if(pica_value($relRecord, '008E')){
            my @relISIL = pica_values($relRecord, '008E');

            foreach my $relISIL (@relISIL){
              if($known{$relISIL}){
                $relIsNl = 1;
              }
            }
          }

        }
        elsif($activeSource eq "zdb"){
          if(pica_value($relRecord, '017B')){
            my @relISIL = pica_values($relRecord, '017B');

            foreach my $relISIL (@relISIL){
              if( $known{$relISIL} || ($filter && $relISIL eq $filter) ){
                $relIsNl = 1;
              }
            }
          }

        }
        if(pica_value($relRecord, '039E')){
          my @relRelatedTitles = @{ pica_fields($relRecord, '039E') };

          foreach my $relRelatedTitle (@relRelatedTitles){
            my @rt = @{ $relRelatedTitle };
            my $rSubfPos = 0;

            foreach my $subField (@rt){
              if($activeSource eq "gvk" || $activeSource eq "gbvcat"){
                if($subField eq 'ZDB' && $rt[$rSubfPos+1] eq '6'){
                  my $rID = formatZdbId($rt[$rSubfPos+2]);

                  if($rID && $rID eq $id){
                    $isDirectRelation = 1;
                  }
                }
              }
              elsif($activeSource eq "zdb"){
                if($subField eq '0'){
                  my $rID = formatZdbId($rt[$rSubfPos+1]);

                  if($rID && $rID eq $id){
                    $isDirectRelation = 1;
                  }
                }
              }
              $rSubfPos++;
            }
          }

          if(pica_value($relRecord, '025@a')){
            $relName = pica_value($relRecord, '025@a');
            if(index($relName, '@') <= 5){
              $relName =~ s/@//;
            }
          }
          elsif(pica_value($relRecord, '021Aa')){
            $relName = pica_value($relRecord, '021Aa');
            if(index($relName, '@') <= 5){
              $relName =~ s/@//;
            }
          }
          $relObj{'title'} = $relName ? $relName : "";

          push @{ $relObj{'identifiers'} }, { 'type' => "zdb", 'value' => $relatedID };

          if($activeSource eq "gvk" || $activeSource eq "natliz" || $activeSource eq "gbvcat" || $activeSource eq "ebp"){
            if(pica_value($relRecord, '004V0')){
              push @{ $relObj{'identifiers'} }, { 'type' => "doi", 'value' => pica_value($relRecord, '004V0') };
            }
          }
          elsif(pica_value($relRecord, '004P0')){
            push @{ $relObj{'identifiers'} }, { 'type' => "doi", 'value' => pica_value($relRecord, '004P0') };
          }

          if(pica_value($relRecord, '005A0')){
            my @relatedISSNs = @{ pica_fields($relRecord, '005A')};

            foreach my $relatedISSN (@relatedISSNs){
              my @rISSN = @{ $relatedISSN };
              my $rSubfPos = 0;

              foreach my $subField (@rISSN){
                if($subField eq '0'){
                  my $formatedRelISSN = formatISSN($rISSN[$rSubfPos+1]);

                  if($formatedRelISSN ne ""){
                    push @{ $relObj{'identifiers'} }, { 'type' => "eissn", 'value' => $formatedRelISSN };
                  }
                }
                $rSubfPos++;
              }
            }
          }

          if(pica_value($relRecord, '005P0')){
            my @relatedISSNs = @{ pica_fields($relRecord, '005P')};

            foreach my $relatedISSN (@relatedISSNs){
              my @rISSN = @{ $relatedISSN };
              my $rSubfPos = 0;

              foreach my $subField (@rISSN){
                if($subField eq '0'){
                  my $formatedRelISSN = formatISSN($rISSN[$rSubfPos+1]);

                  if($formatedRelISSN ne ""){
                    push @{ $relObj{'identifiers'} }, { 'type' => "issn", 'value' => $formatedRelISSN };
                  }
                }
                $rSubfPos++;
              }
            }
          }
        }

        if($relationType && $relationType ne ( 'Druckausg' || 'Druckausg.' )){
          push @relatedPrev, $relatedID;
        }

        if($globalIDs{$relatedID}
          && ref($globalIDs{$relatedID}{'connected'}) eq 'ARRAY'
        ){
          @connectedIDs = @{ $globalIDs{$relatedID}{'connected'} };
        }
      }
      else{
        $logger->debug("did not find related record!");

        push @{ $titleWarnings{'all'} }, {
          '039E' => $relatedID,
          'comment' => "Verknüpfter Titel nicht gefunden -> Print?"
        };
        push @{ $titleWarnings{'zdb'} }, {
          '039E' => $relatedID,
          'comment' => "Verknüpfter Titel nicht gefunden -> Print?"
        };
      }

      if($relRecord && $isDirectRelation == 0){
        $logger->debug("no connected relation!");

        push @{ $titleWarnings{'all'} }, {
          '039E' => $relatedID,
          'comment' => "Titel ist nicht beidseitig verknüpft!"
        };
        push @{ $titleWarnings{'zdb'} }, {
          '039E' => $relatedID,
          'comment' => "Titel ist nicht beidseitig verknüpft!"
        };
      }

      if($relRecord && $relIsNl == 0){
        if($titleStats{'nonNlRelation'}){
          $titleStats{'nonNlRelation'}++;
        }
        else{
          $titleStats{'nonNlRelation'} = 1;
        }
        $logger->debug("Related title not in known packages: $relatedID!");

        unless(any {$_ eq $relatedID} @unknownRelIds){
          push @unknownRelIds, $relatedID;
        }
      }
    }
    if(  $relatedID
      && $relationType
      && $relationType !~ /Druckausg/
      && $isDirectRelation == 1
      && $relIsNl == 1
    ){
      $logger->debug("Trying to add relation to $relatedID");
      $logger->debug("RelType: $relationType");

      my @precedingTypes = ('f','Vorg.','Darin aufgeg.','Hervorgeg. aus');
      my @procedingTypes = ('s','Nachf.','Forts.','Aufgeg. in','Fortgesetzt durch');

      if(any {$_ eq $relationType} @precedingTypes){
        push @{ $titleInfo{'historyEvents'} } , {
            'date' => convertToTimeStamp($start_year, 0),
            'from' => [\%relObj],
            'to' => [{
                'title' => $titleInfo{'name'},
                'identifiers' => $titleInfo{'identifiers'}
            }]
        };
        $logger->debug("Added relation!");

        if($titleStats{'usefulRelated'}){
          $titleStats{'usefulRelated'}++;
        }
        else{
          $titleStats{'usefulRelated'} = 1;
        }
      }
      elsif(any { $_ eq $relationType } @procedingTypes){
        push @{ $titleInfo{'historyEvents'} } , {
            'date' => convertToTimeStamp(($rStartYear ? $rStartYear : $end_year), ($rStartYear ? 0 : 1)),
            'to' => [\%relObj],
            'from' => [{
                'title' => $titleInfo{'name'},
                'identifiers' => $titleInfo{'identifiers'}
            }]
        };
        $logger->debug("Added relation!");
        if($titleStats{'usefulRelated'}){
          $titleStats{'usefulRelated'}++;
        }
        else{
          $titleStats{'usefulRelated'} = 1;
        }
      }
      elsif($rStartYear){
        $logger->debug("Trying to add by dates!");
        if($rEndYear){
          if($rEndYear < $start_year){ # Vorg.
            push @{ $titleInfo{'historyEvents'} } , {
                'date' => convertToTimeStamp($start_year, 0),
                'from' => [\%relObj],
                'to' => [{
                    'title' => $titleInfo{'name'},
                    'identifiers' => $titleInfo{'identifiers'}
                }]
            };
            if($titleStats{'usefulRelated'}){
              $titleStats{'usefulRelated'}++;
            }
            else{
              $titleStats{'usefulRelated'} = 1;
            }
          }
          else{
            if($end_year != 0){
              if($rEndYear <= $end_year){ # Vorg.
                push @{ $titleInfo{'historyEvents'} } , {
                    'date' => convertToTimeStamp($rEndYear, 1),
                    'from' => [%relObj],
                    'to' => [{
                        'title' => $titleInfo{'name'},
                        'identifiers' => $titleInfo{'identifiers'}
                    }]
                };
                if($titleStats{'usefulRelated'}){
                  $titleStats{'usefulRelated'}++;
                }
                else{
                  $titleStats{'usefulRelated'} = 1;
                }
              }
              else{ # Nachf.
                push @{ $titleInfo{'historyEvents'} } , {
                    'date' => convertToTimeStamp($end_year, 1),
                    'to' => [\%relObj],
                    'from' => [{
                        'title' => $titleInfo{'name'},
                        'identifiers' => $titleInfo{'identifiers'}
                    }]
                };
                if($titleStats{'usefulRelated'}){
                  $titleStats{'usefulRelated'}++;
                }
                else{
                  $titleStats{'usefulRelated'} = 1;
                }
              }
            }
            else{ # Vorg.
              push @{ $titleInfo{'historyEvents'} } , {
                  'date' => convertToTimeStamp($rEndYear, 1),
                  'from' => [\%relObj],
                  'to' => [{
                      'title' => $titleInfo{'name'},
                      'identifiers' => $titleInfo{'identifiers'}
                  }]
              };

              if($titleStats{'usefulRelated'}){
                $titleStats{'usefulRelated'}++;
              }
              else{
                $titleStats{'usefulRelated'} = 1;
              }
            }
          }
          $logger->debug("Added relation!");
        }
        else{
          if($end_year != 0){ # Nachf.
            push @{ $titleInfo{'historyEvents'} } , {
                'date' => convertToTimeStamp($end_year, 1),
                'to' => [\%relObj],
                'from' => [{
                    'title' => $titleInfo{'name'},
                    'identifiers' => $titleInfo{'identifiers'}
                }]
            };

            if($titleStats{'usefulRelated'}){
              $titleStats{'usefulRelated'}++;
            }
            else{
              $titleStats{'usefulRelated'} = 1;
            }

            if($rStartYear && $rStartYear < $start_year){ # Vorg.
              push @{ $titleInfo{'historyEvents'} } , {
                  'date' => convertToTimeStamp($start_year, 0),
                  'from' => [\%relObj],
                  'to' => [{
                      'title' => $titleInfo{'name'},
                      'identifiers' => $titleInfo{'identifiers'}
                  }]
              };
            }
            $logger->debug("Added relation!");
          }
          else{
            $logger->warn("Konnte Verknüpfungstyp in $id nicht identifizieren:");
            $logger->warn("Titel: $start_year-".($end_year != 0 ? $end_year : ""));
            $logger->warn("Verknüpft: ".($rStartYear ? $rStartYear : "")."-".($rEndYear ? $rEndYear : ""));
          }
        }
      }
      else{
        $logger->warn("Konnte Verknüpfungstyp in $id nicht identifizieren:");
        $logger->warn("Titel $id: $start_year-".($end_year != 0 ? $end_year : ""));
        $logger->warn("Verknüpft $relatedID: ".($rStartYear ? $rStartYear : "")."-".($rEndYear ? $rEndYear : ""));
      }
    }
  }

  # -------------------- TIPPS (Online-Ressourcen) --------------------

  my @onlineSources;
  my $packagePlatformName = $pkgInfo{'nominalPlatform'}{'name'};
  my $packagePlatformUrl = $pkgInfo{'nominalPlatform'}{'primaryUrl'};
  my $provider = $pkgInfo{'nominalProvider'};

  if($activeSource eq "gvk" || $activeSource eq "gbvcat"){
    if(pica_value($titleRecord, '009P[03]')){
      push @onlineSources, @{pica_fields($titleRecord, '009P[03]')};
    }
    if(pica_value($titleRecord, '009P[05]')){
      push @onlineSources, @{pica_fields($titleRecord, '009P[05]')};
    }
  }
  elsif($activeSource eq "zdb"){
    @onlineSources = @{ pica_fields($titleRecord, '009Q') };
  }
  elsif($activeSource eq "natliz"){
    if(pica_value($titleRecord, '009Q')){
      push @onlineSources, @{ pica_fields($titleRecord, '009Q') };
    }
    if(pica_value($titleRecord, '009P[03]')){
      push @onlineSources, @{pica_fields($titleRecord, '009P[03]')};
    }
    if(pica_value($titleRecord, '009P[05]')){
      push @onlineSources, @{pica_fields($titleRecord, '009P[05]')};
    }
  }
  elsif($activeSource eq "fid") {
    if(pica_value($titleRecord, '009P[03]')){
      push @onlineSources, @{pica_fields($titleRecord, '009P[03]')};
    }
    if(pica_value($titleRecord, '009P[05]')){
      push @onlineSources, @{pica_fields($titleRecord, '009P[05]')};
    }
  }
  elsif($activeSource eq "ebp") {
    if(pica_value($titleRecord, '017C')) {
      push @onlineSources, @{pica_fields($titleRecord, '017C')};
    }
  }

  if($activeSource eq "fid"){
    my %tipp;
    my $pkgIsil = $pkgInfo{'identifiers'}[0]{'value'};
    my $packageItem;
    my @url_items = @{pica_items($titleRecord)};
    my $host;
    my $hostUrl;
    my $tipp_url;
    my $invalid;

    $logger->debug("$pkgIsil");

    foreach my $item (@url_items){

      if (pica_value($item, '209Oa')) {
        my $item_isil = pica_value($item, '209Oa');
        $item_isil =~ s/^\s+|\s+$//g;

        $logger->debug(" Item ISIL: $item_isil");
        if ($item_isil && $item_isil eq $pkgIsil) {
          my $final_url;
          $packageItem = $item;
          my @urls = @{pica_fields($packageItem, '209R[01]')};

          foreach my $urlObject (@urls) {
            my @urlObject = @{$urlObject};
            my $validComment = 0;
            my $subfPos = 0;
            my $urlCandidate;

            foreach my $subField (@urlObject){
              if ($subField eq 'y' && $urlObject[$subfPos+1] ne 'Registrierungslink') {
                $validComment = 1;
              }
              if ($subField eq 'a') {
                $urlCandidate = $urlObject[$subfPos+1];
              }
              $subfPos++;
            }

            if ($validComment) {
              $tipp_url = $urlCandidate;
              $logger->debug("Exemplar-URL: $tipp_url");
            }
          }

          my $uri = URI->new($tipp_url);

          if($uri && $uri->has_recognized_scheme){

            $host = $uri->authority;
            my $scheme = $uri->scheme;
            $tipp{'url'} = $tipp_url;

            if(!$host){
              if($titleStats{'brokenURL'}){
                $titleStats{'brokenURL'}++;
              }
              else{
                $titleStats{'brokenURL'} = 1;
              }
              push @{ $titleWarnings{'all'} }, {
                '209R[01]a'=> $tipp_url,
                'comment' => 'Aus der URL konnte kein Host ermittelt werden.'
              };

              push @{ $titleWarnings{'gvk'} }, {
                '209R[01]a'=> $tipp_url,
                'comment' => 'Aus der URL konnte kein Host ermittelt werden.'
              };

              $logger->error("Could not extract host of URL $tipp_url");
            }
            else{
              $hostUrl = "$scheme://$host";

              $tipp{'platform'} = {
                'name' => lc (substr($host, 0, 3) eq 'www' ? substr($host, 4) : $host),
                'primaryUrl' => $hostUrl
              };

              $tipp{'coverage'} = [];

              my %dates = (
                  'startj' => (pica_value($packageItem, '231@j') || '0'),
                  'startm' => (pica_value($packageItem, '231@c') || '0'),
                  'startd' => (pica_value($packageItem, '231@b') || '0'),
                  'endj' => (pica_value($packageItem, '231@k') || '0'),
                  'endm' => (pica_value($packageItem, '231@m') || '0'),
                  'endd' => (pica_value($packageItem, '231@l') || '0')
              );

              my @dts = transformDate(\%dates);

              push @{ $tipp{'coverage'} }, {
                'startDate' => convertToTimeStamp($dts[0][0], 0),
                'startVolume' => (pica_value($packageItem, '231@d') || ""),
                'startIssue' => (pica_value($packageItem, '231@e') || ""),
                'endDate' => convertToTimeStamp($dts[0][1], 1),
                'endVolume' => (pica_value($packageItem, '231@n') || ""),
                'endIssue' => (pica_value($packageItem, '231@o') || ""),
                'coverageDepth' => "fulltext",
                'coverageNote' => ""
              };

              $tipp{'title'} = \%titleInfo;

              push @tipps, \%tipp;
            }
          }
          else {
            $logger->warn("Looks like a wrong URL!");

            if($titleStats{'brokenURL'}){
              $titleStats{'brokenURL'}++;
            }
            else{
              $titleStats{'brokenURL'} = 1;
            }

            push @{ $titleWarnings{'all'} }, {
              '209R[01]a'=> $tipp_url,
              'comment' => 'URL-Schema konnte nicht ermittelt werden!'
            };

            push @{ $titleWarnings{'gvk'} }, {
              '209R[01]a'=> $tipp_url,
              'comment' => 'URL-Schema konnte nicht ermittelt werden!'
            };

            $logger->error("Could not extract scheme of URL $tipp_url");
          }
        }
        else {
          $logger->debug("Exemplar bezieht sich auf anderes Paket $item_isil.");
        }
      }
    }
  }

  if ($activeSource ne "fid" || scalar @tipps == 0) {

    my $numSources = scalar @onlineSources;
    my @skippedTipps = ();
    my @validTipps = ();

    foreach my $eSource (@onlineSources){

      my ($tipp, $tippWarnings, $tippStats, $tippComments) = processTipp($eSource, $pkgType, $gokbType, $activeSource, $pkgInfo{'nominalPlatform'}, %titleInfo);

      my %tipp = %{$tipp};
      my %tippWarnings = %{$tippWarnings};
      my %tippStats = %{$tippStats};
      my %tippComments = $tippComments ? %{$tippComments} : undef;

      if( ($activeSource eq "gvk" || $activeSource eq "gbvcat" || $activeSource eq "natliz") && pica_value($titleRecord, '008E') ) {
        my $pkgIsil = $pkgInfo{'identifiers'}[0]{'value'};
        my @pkgLinks = pica_fields($titleRecord, '008E');

        foreach my $pkgLink (@pkgLinks) {

          my @pkgLink = @{$pkgLink};
          my $subfPos = 0;
          my $linkedIsil;
          my $retired;

          foreach my $subField (@pkgLink){
            if ($subField eq 'p') {
              $retired = 1;
            }
            elsif ($subField eq 'a') {
              $linkedIsil = $pkgLink[$subfPos+1];
            }
          }

          if ($linkedIsil && $linkedIsil eq $pkgIsil && $retired == 1) {
            $tipp{'status'} = "Retired";
          }
        }
      }

      if(%tippComments) {
        $tipp{'licence'} = $tippComments{'public'} ? $tippComments{'public'} : $tippComments{'licence'};
      }
      else {
        $tipp{'licence'} = 'unknown';
      }
      $tipp{'type'} = $tippComments{'internal'};

      if(!$tipp{'action'}){
        push @validTipps, \%tipp;
      }
      elsif ($tipp{'action'} eq "skipped") {
        delete $tipp{'action'};

        push @skippedTipps, \%tipp;
      }

      if($titleWarnings{'id'} ne "" ){
        push @{ $titleWarnings{'all'} }, @{ $tippWarnings{'all'} };
        push @{ $titleWarnings{'zdb'} }, @{ $tippWarnings{'zdb'} };
        push @{ $titleWarnings{'gvk'} }, @{ $tippWarnings{'gvk'} };
      }

      foreach my $tippStat (keys %tippStats){
        if (!$titleStats{$tippStat}){
          $titleStats{$tippStat} = $tippStats{$tippStat};
        }
        else{
          $titleStats{$tippStat} += $tippStats{$tippStat};
        }
      }

    }
  # End TIPP

    # -------------------- Select URLs ... --------------------

    $logger->info("Relevante TIPPs: ".scalar @validTipps." - Sonstige: ".scalar @skippedTipps);

    if(scalar @validTipps > 0){

      my %remainingValidTipps;

      foreach my $validTipp (@validTipps) {
        my %vTipp = %{$validTipp};
        my $tippType = $vTipp{'type'};
        my $ppBase;

        if($packagePlatformUrl) {
          eval {
            $ppBase = URI->new( $packagePlatformUrl );
            $ppBase = $ppBase->authority();
          }
        }

        if($ppBase) {
          $logger->debug("Base is: $ppBase");
        }

        if($ppBase) {
          my @ppBaseArray = split(/\./, $ppBase);
          $logger->debug("Base Array length is ".scalar @ppBaseArray);

          if($ppBase =~ $vTipp{'platform'}{'name'} || $vTipp{'platform'}{'name'} =~ $ppBase){
            $logger->info("URL mit Paketbasis $ppBase gefunden (TIPP-Plattform ist ".$vTipp{'platform'}{'name'}.").");
            delete $vTipp{'comment'};
            delete $vTipp{'licence'};
            delete $vTipp{'type'};
            push @tipps, \%vTipp;
          }
          elsif(scalar @ppBaseArray > 2) {
            splice @ppBaseArray, 0,1;
            $ppBase = join('.', @ppBaseArray);
            $logger->debug("NO Base URL match! Trying $ppBase ..");

            if($vTipp{'platform'}{'name'} =~ $ppBase) {
              $logger->info("Found plattform by shortened URL!");
              delete $vTipp{'comment'};
              delete $vTipp{'licence'};
              delete $vTipp{'type'};
              push @tipps, \%vTipp;
            }
            else{
              $logger->info("Could not match $ppBase and ".$vTipp{'platform'}{'name'});

              if(!$tippType) {
                push @skippedTipps, \%vTipp;
              }
              else {
                if (ref($remainingValidTipps{$tippType}) eq 'ARRAY'){
                  push @{$remainingValidTipps{$tippType}}, \%vTipp;
                }
                else{
                  $remainingValidTipps{$tippType} = [\%vTipp];
                }
              }
            }
          }
          else{
            $logger->info("Could not match $ppBase with ".$vTipp{'platform'}{'name'});

            if (ref($remainingValidTipps{$tippType}) eq 'ARRAY'){
              push @{$remainingValidTipps{$tippType}}, \%vTipp;
            }
            else{
              $remainingValidTipps{$tippType} = [\%vTipp];
            }
          }
        }
        else {
          if (ref($remainingValidTipps{$tippType}) eq 'ARRAY'){
            push @{$remainingValidTipps{$tippType}}, \%vTipp;
          }
          else{
            $remainingValidTipps{$tippType} = [\%vTipp];
          }
        }
      }

      if(scalar @tipps == 0) {
        if($remainingValidTipps{'H'}){
          $logger->info("Found other valid publisher URL (type 'H')");
          foreach my $selectedValidTipp (@{$remainingValidTipps{'H'}}) {
            my %svTipp = %{$selectedValidTipp};
            delete $svTipp{'comment'};
            delete $svTipp{'licence'};
            delete $svTipp{'type'};
            push @tipps, \%svTipp;
          }
        }
        elsif($remainingValidTipps{'D'}){
          $logger->info("Found valid digitisation URL(s) (type 'D')");
          foreach my $selectedValidTipp (@{$remainingValidTipps{'D'}}) {
            my %svTipp = %{$selectedValidTipp};
            delete $svTipp{'comment'};
            delete $svTipp{'licence'};
            delete $svTipp{'type'};
            push @tipps, \%svTipp;
          }
        }
        elsif($remainingValidTipps{'A'}){
          $logger->info("Found other valid agent URL(s) (type 'A')");
          foreach my $selectedValidTipp (@{$remainingValidTipps{'A'}}) {
            my %svTipp = %{$selectedValidTipp};
            delete $svTipp{'comment'};
            delete $svTipp{'licence'};
            delete $svTipp{'type'};
            push @tipps, \%svTipp;
          }
        }
        elsif($remainingValidTipps{'C'}){
          $logger->info("Found other valid archival URL(s) (type 'C')");
          foreach my $selectedValidTipp (@{$remainingValidTipps{'C'}}) {
            my %svTipp = %{$selectedValidTipp};
            delete $svTipp{'comment'};
            delete $svTipp{'licence'};
            delete $svTipp{'type'};
            push @tipps, \%svTipp;
          }
        }
        elsif($remainingValidTipps{'L'}){
          $logger->info("Found other valid long-time archival URL(s) (type 'L')");
          foreach my $selectedValidTipp (@{$remainingValidTipps{'L'}}) {
            my %svTipp = %{$selectedValidTipp};
            delete $svTipp{'comment'};
            delete $svTipp{'licence'};
            delete $svTipp{'type'};
            push @tipps, \%svTipp;
          }
        }
        elsif($remainingValidTipps{'G'}){
          $logger->info("Found other valid aggregator URL(s) (type 'G')");
          foreach my $selectedValidTipp (@{$remainingValidTipps{'G'}}) {
            my %svTipp = %{$selectedValidTipp};
            delete $svTipp{'comment'};
            delete $svTipp{'licence'};
            delete $svTipp{'type'};
            push @tipps, \%svTipp;
          }
        }
      }
    }

    if(scalar @tipps == 0 && scalar @skippedTipps > 0){

      foreach my $skTipp (@skippedTipps) {
        my %skTipp = %{$skTipp};
        $logger->info("Looking for OA-URL");
        if ( scalar @tipps == 0 && ($skTipp{'licence'} eq "LF" || $skTipp{'licence'} eq "KF" || $skTipp{'licence'} eq "KW") ) {
          delete $skTipp{'comment'};
          delete $skTipp{'licence'};
          delete $skTipp{'type'};
          push @tipps, \%skTipp;

          if($titleStats{'numUsedOA'}){
            $titleStats{'numUsedOA'}++;
          }
          else{
            $titleStats{'numUsedOA'} = 1;
          }
        }
      }
      if ( scalar @tipps == 0) {
        $logger->info("Looking for other Publisher-URLs");
        foreach my $skTipp (@skippedTipps) {
          my %skTipp = %{$skTipp};

          if ($skTipp{'type'} eq "H" || $skTipp{'type'} eq "") {
            delete $skTipp{'comment'};
            delete $skTipp{'licence'};
            delete $skTipp{'type'};
            if ( scalar @tipps > 0 ) {
              $logger->info("Got more than one Publisher-URL..");
            }
            else {
              push @tipps, \%skTipp;
            }
          }
        }
      }
    }

    if (scalar @tipps == 0){

      # Add placeholder TIPP

      if($titleStats{'numNoUrl'}){
        $titleStats{'numNoUrl'}++;
      }
      else{
        $titleStats{'numNoUrl'} = 1;
      }
      $logger->warn("No valid URL found, adding placeholder ($id)!");

      push @{ $titleWarnings{'all'} }, {
          '009P0'=> "Keine relevanten URLs identifiziert!"
      };

      push @{ $titleWarnings{'gvk'} }, {
          '009P0'=> "Keine relevanten URLs identifiziert!"
      };

      if($packagePlatformName){
        push @tipps, {
          'medium' => "Electronic",
          'platform' => {
            'name' => $packagePlatformName ? $packagePlatformName : "",
            'primaryUrl' => $packagePlatformUrl
          },
          'url' => $packagePlatformUrl,
          'status' => "Current",
          'title' => \%titleInfo
        }
      }
      else{
        push @tipps, {
          'medium' => "Electronic",
          'platform' => {
            'name' => $packagePlatformUrl,
            'primaryUrl' => ""
          },
          'url' => $packagePlatformUrl,
          'status' => "Current",
          'title' => \%titleInfo
        }
      }
    }
  }
  # -------------------- Warnings --------------------

  if(scalar @{$titleWarnings{'all'}} > 0){

    push @{ $titleWarnings{'all'} }, {
        'title' => $titleInfo{'name'}
    };
  }

  if(scalar @{$titleWarnings{'zdb'}} > 0){
    push @{ $titleWarnings{'zdb'} }, {
        'title' => $titleInfo{'name'}
    };
  }

  if(scalar @{$titleWarnings{'gvk'}} > 0){
    push @{ $titleWarnings{'gvk'} }, {
        'title' => $titleInfo{'name'}
    };
  }
  if($titleStats{'titlesTotal'}){
    $titleStats{'titlesTotal'}++;
  }
  else{
    $titleStats{'titlesTotal'} = 1;
  }

  # -------------------- Collect IDs --------------------

  unless($globalIDs{$id}){
    $globalIDs{$id} = {
        'eissn' => \@eissn,
        'title' => $titleInfo{'name'},
        'connected' => \@relatedPrev
    };
    push @allTitles , \%titleInfo;
  }
  else{
    $logger->warn("ID ".$id." ist bereits in der Titelliste vorhanden!");

    if($titleStats{'duplicateZDBids'}){
      $titleStats{'duplicateZDBids'}++;
    }
    else{
      $titleStats{'duplicateZDBids'} = 1;
    }
  }
  return \@tipps, \%titleStats, \%titleWarnings, \%titleInfo;

} ## End TitleInstance


  ################ TIPP ################


sub processTipp {
  my %tipp;
  my ($eSource, $pkgType, $gokbType, $activeSource, $platformInfo, %titleInfo) = @_;
  my @eSource = @{$eSource};
  my %tippStats;

  my %tippWarnings = (
    'all' => [],
    'zdb' => [],
    'gvk' => []
  );

  my $sourceURL = "";
  my $viableURL = 0;
  my $internalComments = "";
  my $isNL = 0;
  my $toSkip;
  my %validInternalComments = (
    'H' => {
      'gvk' => 'Verlag',
      'zdb' => 'H;'
    },
    'D' => {
      'gvk' => 'Digitalisierung',
      'zdb' => 'D;'
    },
    'A' => {
      'gvk' => 'Agentur',
      'zdb' => 'A;'
    },
    'C' => {
      'gvk' => 'Archivierung',
      'zdb' => 'C;'
    },
    'L' => {
      'gvk' => 'Langzeitarchivierung',
      'zdb' => 'L;'
    },
    'G' => {
      'gvk' => 'Aggregator',
      'zdb' => 'G;'
    },
    'R' => {
      'gvk' => 'Resolving-System',
      'zdb' => 'R;'
    }
  );

  my %tippComments = (
    'public' => "",
    'internal' => "",
    'licence' => ""
  );

  my $subfPos = 0;


  foreach my $subField (@eSource){
    if($subField eq 'a' || $subField eq 'u'){
      $sourceURL = $eSource[$subfPos+1];

      if(index($sourceURL, '=u ') == 0){
        $sourceURL =~ s/=u\s//;
      }

      if($sourceURL =~ /http\/\//){
        push @{ $tippWarnings{'all'} }, {
          '009P0'=> $sourceURL,
          'comment' => "URL ist ungültig (':' nach http fehlt)"
        };
        push @{ $tippWarnings{'gvk'} }, {
          '009P0'=> $sourceURL,
          'comment' => "URL ist ungültig (':' nach http fehlt)"
        };

        if($tippStats{'nlURLs'}){
          $tippStats{'nlURLs'}++;
        }
        else{
          $tippStats{'nlURLs'} = 1;
        }
        $sourceURL =~ s/http\/\//http:\/\//;
      }
    }
    elsif($subField eq 'x'){
      $internalComments = $eSource[$subfPos+1];

    }
    elsif($subField eq 'z'){
      $tippComments{'public'} = $eSource[$subfPos+1];
      $logger->debug("Found public comment ".$tippComments{'public'}."..");

    }
    elsif($subField eq '4'){
      $tippComments{'licence'} = $eSource[$subfPos+1];
    }
    $subfPos++;
  }

  if(!$sourceURL || length $sourceURL > 255 || $sourceURL eq ""){
    $logger->error("Skipping TIPP with no or overlong URL!");
    $tipp{'action'} = "error";
    return (\%tipp, \%tippWarnings, \%tippStats);
  }
  else{
    $logger->debug("Considering URL: $sourceURL");
  }

  my $internalCommentIsValid = 0;

  if($activeSource eq "natliz" && $gokbType ne "Serial"){
    if($pkgType eq "NL"){
      $tippComments{'public'} = "NL";
    }
  }

  if($activeSource eq "gvk" || $activeSource eq "gbvcat" || $activeSource eq "ebp"){
    while (my ($uType, $vCom) = each %validInternalComments ){
      my %vCom = %{$vCom};

      if( $internalComments =~ $vCom{'gvk'} ){
        $internalCommentIsValid = 1;
        $tippComments{'internal'} = $uType;
      }
    }

    if(!$internalComments){
      $internalCommentIsValid = 1;
    }

  }
  elsif($activeSource eq "zdb"){
    while (my ($uType, $vCom) = each %validInternalComments ){
      my %vCom = %{$vCom};

      if( $internalComments =~ $vCom{'zdb'} || $internalComments eq $uType ){
        $tippComments{'internal'} = $uType;
        $internalCommentIsValid = 1;
      }
    }
  }
  elsif($activeSource eq "natliz"){
    $internalCommentIsValid = 1;
  }

  if($pkgType eq "NL"){
    if($tippComments{'public'} ne "Deutschlandweit zugänglich" && $tippComments{'public'} ne "NL"){
      if($tippStats{'otherURLs'}){
        $tippStats{'otherURLs'}++;
      }
      else{
        $tippStats{'otherURLs'} = 1;
      }

      $logger->debug("Skipping NL-TIPP.. wrong Public Comment: ",$tippComments{'public'},", (internal=$internalComments)");
      $tipp{'action'} = "skipped";
    }
    else{
      if($tippStats{'nlURLs'}){
        $tippStats{'nlURLs'}++;
      }
      else{
        $tippStats{'nlURLs'} = 1;
      }
      $logger->debug("Using NL-TIPP.. Public Comment: ",$tippComments{'public'},", (internal=$internalComments)");
      $isNL = 1;
    }


    if($internalCommentIsValid != 1){
      $logger->debug("Skipping NL-TIPP.. wrong Internal Comment: $internalComments");
      $tipp{'action'} = "skipped";
    }
  }
  else {
    if($internalCommentIsValid == 1 && $tippComments{'public'} ne "Deutschlandweit zugänglich" && $tippComments{'public'} ne "NL"){

      if($tippStats{'otherURLs'}){
        $tippStats{'otherURLs'}++;
      }
      else{
        $tippStats{'otherURLs'} = 1;
      }

      if($tippComments{'public'} eq "LF") {
        $tipp{'paymentType'} = "OA";
      }

    }
    else{
      $logger->debug("Skipping TIPP .. wrong Type or source: $internalComments, ",$tippComments{'public'});
      $tipp{'action'} = "skipped";
    }
  }

  $tipp{'status'} = "Current";
  $tipp{'medium'} = "Electronic";
  $tipp{'accessStart'} = "";
  $tipp{'accessEnd'} = "";
  $tipp{'url'} = $sourceURL;

  # -------------------- Platform --------------------

  my $url = URI->new( $sourceURL );
  my $host;
  my $hostUrl = "";

  if($url->has_recognized_scheme){

    $host = $url->authority;
    my $scheme = $url->scheme;

    if(!$host && !$tipp{'action'}){
      if($tippStats{'brokenURL'}){
        $tippStats{'brokenURL'}++;
      }
      else{
        $tippStats{'brokenURL'} = 1;
      }
      push @{ $tippWarnings{'all'} }, {
        '009P0'=> $sourceURL,
        'comment' => 'Aus der URL konnte kein Host ermittelt werden.'
      };

      push @{ $tippWarnings{'zdb'} }, {
        '009Qx'=> $sourceURL,
        'comment' => 'Aus der URL konnte kein Host ermittelt werden.'
      };

      $logger->error("Could not extract host of URL $url");
      $tipp{'action'} = "error";
      return (\%tipp, \%tippWarnings, \%tippStats, \%tippComments);
    }
    else{
      $hostUrl = "$scheme://$host";
    }
  }
  elsif (!$tipp{'action'}) {
    $logger->warn("Looks like a wrong URL!");

    if($tippStats{'brokenURL'}){
      $tippStats{'brokenURL'}++;
    }
    else{
      $tippStats{'brokenURL'} = 1;
    }

    push @{ $tippWarnings{'all'} }, {
      '009P0'=> $sourceURL,
      'comment' => 'URL-Schema konnte nicht ermittelt werden!'
    };

    push @{ $tippWarnings{'zdb'} }, {
      '009Qx'=> $sourceURL,
      'comment' => 'URL-Schema konnte nicht ermittelt werden!'
    };

    $logger->error("Could not extract scheme of URL $url");
    $tipp{'action'} = "error";

    return (\%tipp, \%tippWarnings, \%tippStats, \%tippComments);
  }

  if ($host =~  /doi\.org/ && $platformInfo->{'name'}) {
    $tipp{'platform'} = \%{$platformInfo};
  }
  elsif (!$platformInfo->{'name'}) {
    $logger->debug("Resolving DOI URL");
    $tipp{'platform'} = resolveTippPlatform($url);
  }
  else {
    $tipp{'platform'} = {
      'name' => lc (substr($host, 0, 3) eq 'www' ? substr($host, 4) : $host),
      'primaryUrl' => $hostUrl
    };
  }

  # -------------------- Coverage --------------------

  my $startVol = "";
  my $startIss = "";
  my $startDate = "";
  my $endVol = "";
  my $endIss = "";
  my $endDate = "";
  my $covNote = "";


  if($internalComments =~ ";"){
    my @fieldParts = split(';', $internalComments);

    if(scalar @fieldParts == 2){

      # Split into start and end
      $covNote = $fieldParts[1];

      my @datesParts = split /\-/, $fieldParts[1];
      my $datePartPos = 0;

      foreach my $dp (@datesParts){
        if( $dp ne " " && ($dp =~ /[a-zA-Z]+/
            ||
            $dp !~ /^\s?[0-9]*\.?[0-9]{4},?[0-9]*\s?$/)
        ){
          push @{ $tippWarnings{'all'} }, {
            '009P[05]'=> $fieldParts[1]
          };
          push @{ $tippWarnings{'zdb'} }, {
            '009Qx'=> $fieldParts[1]
          };
        }

        my ($tempVol) = $dp =~ /([0-9]+)\.[0-9]{4}/;
        my ($sos, $tempYear, $eos) = $dp =~ m/(^|\s|\.)([0-9]{4})(\/|,|\s|$)/g;
        my ($tempIss) = $dp =~ /,([0-9]+)\s*$/;

        # Date

        if($tempYear && $tempYear ne ""){

          if($datePartPos == 0){
            $startDate = convertToTimeStamp($tempYear, 0);
          }
          else{
            $endDate = convertToTimeStamp($tempYear, 1);
          }
        }

        # Volume

        if($tempVol && $tempVol ne ""){
          if($datePartPos == 0){
            $startVol = $tempVol;
          }
          else{
            $endVol = $tempVol;
          }
        }
        # Issue

        if($tempIss && $tempIss ne ""){
          if($datePartPos == 0){
            $startIss = $tempIss;
          }
          else{
            $endIss = $tempIss;
          }
        }
        $datePartPos++;
      }
    }
  }

  $tipp{'coverage'} = [];
  push @{ $tipp{'coverage'} } , {
    'startDate' => $startDate ? $startDate : "",
    'startVolume' => $startVol,
    'startIssue' => $startIss,
    'endDate' => $endDate ? $endDate : "",
    'endVolume' => $endVol,
    'endIssue' => $endIss,
    'coverageDepth' => "Fulltext",
    'coverageNote' => $pkgType ? "$pkgType-DE; $covNote" : "$covNote",
    'embargo' => ""
  };

  if($titleInfo{'ebp'}) {
    $tipp{'identifiers'} = [];

    push @{ $tipp{'identifiers'}} , {
      'type' => 'epb',
      'value' => $titleInfo{'ebp'}
    };
  }

  # -------------------- TitleInstance (in TIPP) --------------------

  $tipp{'title'} = \%titleInfo;

  return (\%tipp, \%tippWarnings, \%tippStats, \%tippComments);
}

# Submit package/title JSON to GOKb-API

sub postData {
  my $endPointType = shift;
  my $data = shift;
  my $endPoint = $gokbCreds{'base'}."integration/".$endPointType;

  if($endPointType eq 'crossReferencePackage' || $endPointType eq 'crossReferenceTitle') {
    $endPoint .= "?async=true";
  }

  if($data){

    my $json_gokb = JSON->new->utf8->canonical;

    my %decData;
    my @decData;

    if (ref($data) eq 'HASH') {
      %decData = %{ $data };
    }
    elsif (ref($data) eq 'ARRAY') {
      @decData = @{ $data };
    }

    my $ua = LWP::UserAgent->new;

    $ua->timeout(7200);

    my $req = HTTP::Request->new(POST => $endPoint);

    $req->header('content-type' => 'application/json');
    $req->header('accept' => 'application/json');
    $req->authorization_basic($gokbCreds{'username'}, $gokbCreds{'password'});

    if(ref($data) eq 'HASH') {
      $req->content($json_gokb->encode( \%decData ));
    }
    else {
      $req->content($json_gokb->encode( \@decData ));
    }

    my $resp = $ua->request($req);

    my $resp_content = decode_json($resp->content);
    my @arrayResponse;
    my $hashResponse;
    my $responseString;
    my $jobId;

    if (ref($resp_content) eq 'ARRAY') {
      @arrayResponse = @{$resp_content};
    }
    elsif (ref($resp_content) eq 'HASH'){
      $hashResponse = $resp_content;
    }
    else {
      $logger->info("Got result: $resp_content");
    }

    if($resp->is_success){

      if($endPointType eq 'crossReferencePackage' || $endPointType eq 'crossReferenceTitle'){
        $jobId = $hashResponse->{'job_id'};

        my $jobUrl = $gokbCreds{'base'}."integration/getJobInfo/$jobId";
        my $jobRec = HTTP::Request->new(GET => $jobUrl);
        my $finished;

        $jobRec->authorization_basic($gokbCreds{'username'}, $gokbCreds{'password'});

        while (!$finished) {
          sleep(2);
          my $jobResp = $ua->request($jobRec);
          my $job_content = decode_json($jobResp->content);

          if ($job_content->{'finished'}) {
            $finished = 1;

            if($job_content->{'cancelled'}) {
              $logger->info("Job cancelled after ".$job_content->{'progress'}."% ");
            }
            else {
              my $results = $job_content->{'job_result'};

              if ($endPointType eq 'crossReferencePackage') {
                if ($results->{'result'} eq 'ERROR'){
                  $logger->error($results->{'message'});
                  foreach my $error (@{$results->{'errors'}}) {
                    $logger->error($error->{'message'});

                    if ($error->{'errors'}) {
                      foreach my $field (keys %{$error->{'errors'}}) {
                        foreach my $fieldError (@{$error->{'errors'}->{$field}}) {
                          $logger->error($fieldError->{'message'});
                        }
                      }
                    }
                  }
                }
                else {
                  $logger->info($results->{'message'});
                }
              }
              else {
                if ($job_content->{'result'} eq 'ERROR') {
                  $logger->error($job_content->${'message'});
                }
                else {
                  my $total = scalar @{$results->{'results'}};
                  my $errors = 0;
                  foreach my $titleResult (@{$results->{'results'}}) {
                    if ($titleResult->{'result'} eq 'ERROR') {
                      $errors += 1;
                      $logger->error($titleResult->{'message'});

                      if ($titleResult->{'errors'} && ref($titleResult->{'errors'}) eq 'ARRAY') {
                        foreach my $error (@{$titleResult->{'errors'}}) {
                          $logger->error($error);
                        }
                      }
                      elsif ($titleResult->{'errors'} && ref($titleResult->{'errors'}) eq 'STRING') {
                        $logger->error($titleResult->{'errors'});
                      }
                      elsif ($titleResult->{'errors'} && ref($titleResult->{'errors'}) eq 'HASH') {
                        foreach my $error ($titleResult->{'errors'}) {
                          print Dumper($error);
                        }
                      }
                    }
                  }
                  $logger->info("Finished sending title data: $total total, $errors errors ..")
                }
              }
            }
          }
          else {
            $logger->info("Processing .. ".$job_content->{'progress'}."% ");
          }
        }
      }
      elsif($hashResponse && $hashResponse->{'results'}) {
        foreach my $responseItem (@{$hashResponse->{'results'}}) {
          if ($responseItem->{'result'} eq 'ERROR') {
            $logger->error($responseItem->{'message'});
          }
        }
      }

      return 0;
    }
    else{
      $logger->error("HTTP POST error code: ".$resp->code);

      if($resp->message){
        $logger->error("HTTP POST error message: ".$resp->message);
      }

      if($hashResponse) {
        $logger->error("HTTP POST error message:".$hashResponse->{'message'});

        if ($hashResponse->{'errors'}) {
          foreach my $eObj (@{$hashResponse->{'errors'}}) {
            my %eObj = %{$eObj};
            $logger->error("Error:".$eObj{'message'});
          }
        }
      }
      elsif(@arrayResponse) {
        foreach my $resObj (@arrayResponse) {
          my %resObj = %{$resObj};

          if ($resObj{'result'} eq 'ERROR') {
            $logger->error($resObj{'message'});
          }
        }
      }

      return $resp->code;
    }
  }
  else{
    $logger->error("Wrong endpoint or no data!");

    return -1;
  }
}

# ensure ISSN format

sub formatISSN {
  my ($issn) = shift;

  $issn =~ s/^\s+|\s+$//g;

  if($issn && $issn =~ /^[0-9xX]{4}-?[0-9xX]{4}$/){
    if(index($issn, '-') eq '-1'){
      $issn = join('-', unpack('a4 a4', $issn));
    }

    $issn =~ s/x/X/g;

    my $issnChk = CheckDigits('issn');

    if($issnChk->is_valid($issn)){
      return $issn;
    }
    else{
      return "";
    }
  }
  else{
    return "";
  }
}

# ensure ZDB-ID format

sub formatZdbId {
  my ($id) = shift;

  if($id && $id =~ /^\d*-?[0-9xX]?$/){
    $id =~ s/-//g;
    $id =~ s/x/X/g;

    substr($id, -1, 0, '-');

    return $id;
  }
  else{
    return;
  }
}

sub resolveTippPlatform {
  my $doiUrl = shift;

  unless (%resolvedPlatform) {
    my $resolved = checkUrl($doiUrl);

    $logger->debug("Got revolved URL $resolved");

    if ($resolved) {
      my $url = URI->new( $resolved );
      my $host = $url->authority;
      my $scheme = $url->scheme;

      %resolvedPlatform = (
        name => lc (substr($host, 0, 3) eq 'www' ? substr($host, 4) : $host),
        primaryUrl => "$scheme://$host"
      );
    }
    else {
      $logger->error("Unable to resolve platform URL $doiUrl");
    }
  }

  return \%resolvedPlatform;
}

# look up a provided publisher in ONLD.jsonld

sub matchExistingOrgs {
  my $pubName = shift;
  my $normPubName = normalizeString($pubName);
  my $publisherMatch;

  if(!$matchOrgsByFile){
    if ($gokbCreds{'username'} && $gokbCreds{'password'}) {
      my $ua = LWP::UserAgent->new;
      my %params = ( 'componentType' => "Org", 'label' => $pubName, 'max' => 20 );
      my $url = $gokbCreds{'base'}."api/find?";
      my $uri = URI->new($url);

      $uri->query_form(%params);

      my $req = HTTP::Request->new(GET => $uri);

      # $req->authorization_basic($gokbCreds{'username'}, $gokbCreds{'password'});

      my $resp = $ua->request($req);

      if($resp->is_success){
        my $orgResp = decode_json($resp->content);
        my %orgResp = %{$orgResp};
        my @res = @{$orgResp{'records'}};

        foreach my $record (@res) {
          my %record = %{ $record };
          my $gokbNormOrg = normalizeString($record{'name'});
          if ($gokbNormOrg eq $normPubName) {
            $publisherMatch = $record{'name'};
            last;
          }
          else{
            foreach my $altname (@{$record{'altname'}}){
              my $gokbNormAltName = normalizeString($altname);
              if ($gokbNormAltName eq $normPubName) {
                $publisherMatch = $record{'name'};
                last;
              }
            }
            if($publisherMatch){
              last;
            }
          }
        }
        if($publisherMatch) {
          $logger->debug("Matched GOKb Org $publisherMatch for given Org: $pubName");
        }
        else{
          $logger->debug("Could not find GOKb Org $pubName ..");
        }




      }
      else{
        $logger->warn("Could not look up Org name in GOKb..");
        $logger->warn("HTTP GET error code: ".$resp->code);
        $logger->warn("HTTP GET error message: ".$resp->message);
        $logger->warn("Using ONLD.jsonld from now on..");
        $matchOrgsByFile = 1;
      }
    }
    else{
      $logger->warn("Could not find any GOKb credentials.. matching by file.");
      $matchOrgsByFile = 1;
    }
  }

  if($matchOrgsByFile){

    foreach my $ncsuOrg ( @{ $orgsJSON{'@graph'} } ) {
      my %ncsuOrg = %{ $ncsuOrg };
      my $ncsuPref = $ncsuOrg{'skos:prefLabel'};
      my $ncsuPrefNorm = normalizeString($ncsuPref);

      if($normPubName eq $ncsuPrefNorm) {
        $publisherMatch = $ncsuPref;

        last;

      # Search in ncsu altLabels

      }
      elsif($ncsuOrg{'skos:altLabel'}){
        foreach my $altLabel ( @{ $ncsuOrg{'skos:altLabel'} } ) {
          my $altLabelNorm = normalizeString($altLabel);

          if($normPubName eq $altLabelNorm){
            $publisherMatch = $ncsuPref;

            last;
          }
        }
      }
    }
  }

  if($publisherMatch){
    return $publisherMatch;
  }
  else{
    return;
  }
}

# Normalize String
# note: replicated GOKb string normalization process

sub normalizeString {
  my $origString = shift;
  my $normString = "";
  my @stopWords = ( "and", "the", "from" );
  my $NFD_string = NFD($origString);

  $NFD_string =~ s/\\p\{InCombiningDiacriticalMarks\}\+/ /g;
  $NFD_string = lc($NFD_string);

  my @stringParts = split(/\s/, $NFD_string);

  @stringParts = sort @stringParts;

  foreach my $stringPart (@stringParts){
    unless(any {$_ eq $stringPart} @stopWords){
      $stringPart =~ s/[^a-z0-9]/ /g;
      $normString .= $stringPart;
    }
  }
  $normString =~ s/\s//g;

  return $normString;
}

# Convert Date(part) to Timestamp

sub convertToTimeStamp {
  my ($date, $end) = @_;

  my @parts = split('-', $date);

  if(scalar @parts > 0){
    if(length $parts[0] > 4){
      $parts[0] = substr($parts[0],0,4);
    }

    if(length $parts[0] != 4){
      return "";
    }
  }
  else{
    return "";
  }
  if(scalar @parts == 1){
    if($end == 0){
      $date .= "-01-01";
    }
    elsif($end == 1){
      $date .= "-12-31";
    }
  }
  elsif(scalar @parts == 2){
    if($end == 0){
      $date .= "-01";
    }
    elsif($end == 1){
      $date .= "-31";
    }
  }
  elsif(scalar @parts != 3){
    return "";
  }
  $date .= " 00:00:00.000";

  return $date;
}

sub checkUrl {
  my $url = shift;

  my $mech = WWW::Mechanize->new(autocheck => 0);
  $mech->max_redirect(0);
  $mech->get($url);

  my $status = $mech->status();
  if (($status >= 300) && ($status < 400)) {
    my $location = $mech->response()->header('Location');
    if (defined $location) {
      my $redirectMech = WWW::Mechanize->new(autocheck => 0);
      $redirectMech->get(URI->new_abs($location, $mech->base()));

      if ($redirectMech->status() >= 400) {
        $logger->warn("URL $url is not currently valid!");
        return;
      }
      else{
        my $new_url = $redirectMech->uri();
        $new_url = $new_url->as_string();
        return $new_url;
      }
    }
  }
  elsif($status == 200){
    return $url;
  }
  else{
    $logger->error("URL $url is not currently valid!");
    return;
  }
}

# Create Dates (YYYY-MM-DD) from parts as in [YYYY,MM,DD,YYYY,MM,DD]

sub transformDate {
  my $parts = shift;
  my %parts = %{ $parts };
  my @combined;

  if($parts{'startj'} ne '0'){
    my $corYear = $parts{'startj'};
    if(!looks_like_number($parts{'startj'})){
      $corYear = substr($parts{'startj'}, 0, 4);
    }
    $combined[0] = $corYear;

    if($parts{'startm'} ne '0'){
      $combined[0] .= "-".$parts{'startm'};

      if($parts{'startd'} ne 0){
        $combined[0] .= "-".$parts{'startd'};
      }
    }
  }
  else{
    $combined[0] = "";
  }

  if($parts{'endj'} ne '0'){
    my $corYear = $parts{'endj'};
    if(!looks_like_number($parts{'endj'})){
      $corYear = substr($parts{'endj'}, 0, 4);
    }
    $combined[1] = $corYear;

    if($parts{'endm'} ne '0'){
      $combined[1] .= "-".$parts{'endm'};

      if($parts{'endd'} ne '0'){
        $combined[1] .= "-".$parts{'endd'};
      }
    }
  }
  else{
    $combined[1] = "";
  }

  return \@combined;
}
