#!/usr/bin/perl

# Dieses Script dient der Synchronisation von NL-Paketen mit der GOKb
# Es werden (je nach Aufruf) mehrere Schritte durchlaufen:
# 1. Import der Sigelinformationen aus dem Nationallizenzen-CMS
# 2. Anreicherung der Paketinformationen mit Daten aus dem ZDB-Sigelverzeichnis
# 3. Extrahieren von Titelinformationen über die SRU-Schnittstelle des GBV
# 4. Upload der Paket- und Titeldaten in eine GOKb-Instanz
#
# Parameter:
# --packages "data_source,username,password"
#  * erstellt die known_seals.json
# --json (ZDB-1-...)
#  * neue Methode, known_seals.json muss vorhanden sein
#  * ohne folgendes Paketsigel werden alle Pakete bearbeitet.
# --endpoint
#  * ändert die Datenquelle für Titeldaten
#  * weglassen für Standardbezug über VZG-SRU
#  * Mögliche Werte: "zdb","natliz","gvk" (Standard)
# --post (URL)
#  * folgt keine URL, wird die localhost Standardadresse verwendet
#  * nur zulässig nach --json
# --new_orgs
#  * überträgt gefundene Körperschaften mit GND-ID an die GOKb
#  * funktioniert nur in Verbindung mit --post
# -- pub_type
#  * Schränkt die verarbeitete Materialart ein
#  * Mögliche Werte: 'journal' (Standard), 'book', 'all'

use v5.22;
use strict;
use warnings;
use utf8;
use DBI;
use JSON;
use URI;
use Unicode::Normalize;
use IO::Tee;
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

my $requestedType = "journal";

## Name der JSON-Datei mit Paketinformationen

my $knownSeals = 'CMS_Pakete.json';

## Standard-URL der Ziel-GOKb

my $baseUrl = 'http://localhost:8080/gokb/';
my $filter;


### logging

my $logFnDate = strftime '%Y-%m-%d', localtime;
my $logFn = 'logs_'.$logFnDate.'.log';

$logDir->mkpath( { verbose => 0 } );

my $logFile = $logDir->file($logFn);

$logFile->touch();

my $out_logs = $logFile->opena();

my $tee = new IO::Tee(\*STDOUT, $out_logs);

*STDERR = *$tee{IO};

select $tee;

## Öffne JSON-Datei mit GOKb-Organisationsdaten

my $ncsu_orgs = do {
  open(my $orgs_in, '<' , "ONLD.jsonld")
      or die("Can't open ONLD.jsonld: $!\n");

  local $/;

  <$orgs_in>
};

my %orgsJSON = %{decode_json($ncsu_orgs)}
  or die "Konnte JSON mit NCSU-Orgs nicht dekodieren! \n";

# Check for login configuration

my %cmsCreds;
my %gokbCreds;

if(-e 'login.json'){
  my $login_data = do {
    open(my $logins, '<' , "login.json")
        or die("Can't open login.json: $!\n");

    local $/;

    <$logins>
  };

  my %logins = %{decode_json($login_data)}
    or die "Konnte JSON mit Logins nicht dekodieren! \n";

  if($logins{'cms'}){
    %cmsCreds = %{ $logins{'cms'} };
  }

  if($logins{'gokb'}){
    %gokbCreds = %{ $logins{'gokb'} };
  }
}

# Handle parameters

my $endpoint = "gvk";
my $newOrgs = 0;
my $customTarget;

my $argP = first_index { $_ eq '--packages' } @ARGV;
my $argJ = first_index { $_ eq '--json' } @ARGV;
my $argPost = first_index { $_ eq '--post' } @ARGV;
my $argEndpoint = first_index { $_ eq '--endpoint' } @ARGV;
my $argNewOrgs = first_index { $_ eq '--new_orgs' } @ARGV;
my $argType = first_index { $_ eq '--pub_type' } @ARGV;

if($ARGV[$argPost+1] && index($ARGV[$argPost+1], "http") == 0){
  $gokbCreds{'base'} = $ARGV[$argPost+1];
  $customTarget = 1;
}

if( $argType >= 0) {
  if($ARGV[$argType+1] && any { $_ eq $ARGV[$argType+1] } ("journal","book","all") ) {
    $requestedType = $ARGV[$argType+1];
  }else{
    die "Ungültiger Materialtyp! Möglich sind 'journal'(Standard), 'book' und 'all'";
  }
}

if($argNewOrgs >= 0){
  $newOrgs = 1;
}

if( $argEndpoint >= 0) {
  if($ARGV[$argEndpoint+1] && any { $_ eq $ARGV[$argEndpoint+1] } ("zdb","natliz","gvk") ) {
    $endpoint = $ARGV[$argEndpoint+1];
  }else{
    die "Ungültiger Endpunkt! Möglich sind 'zdb', 'natliz' und 'gvk'(Standard)";
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
    }else{
      die "Falsches Format der DB-Daten! Abbruch!";
    }
  }

  if(!$cmsCreds{'base'} || !$cmsCreds{'username'} || !$cmsCreds{'password'}){
    die "Datenbankinformationen fehlen/falsch! Format ist: \"data_source,username,password\"";
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
        }else{
          say "Kein Benutzername/Passwort, überspringe GOKb-Import!";
        }
      }
      if(index($ARGV[$argJ+1], "ZDB") == 0){
        $filter = $ARGV[$argJ+1];
        createJSON($post, $endpoint, $newOrgs);
      }else{
        say "Pakete abgerufen, erstelle JSONs!";

        createJSON($post, $endpoint, $newOrgs);
      }
    }else{
      die "Erstelle keine JSONs, Sigeldatei wurde nicht erstellt!";
    }
  }else{
      say "Erstelle nur Paketdatei $knownSeals!";

      getSeals($cmsCreds{'base'},$cmsCreds{'username'},$cmsCreds{'password'});
  }
}elsif($argJ >= 0){
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
      }else{
        say "Kein Benutzername/Passwort, überspringe GOKb-Import!";
      }
    }
    if($ARGV[$argJ+1] && index($ARGV[$argJ+1], "ZDB") == 0){
      $filter = $ARGV[$argJ+1];

      say "Paketdatei gefunden, erstelle JSON für $filter!";

      createJSON($post, $endpoint, $newOrgs);
    }else{
      say "Paketdatei gefunden, erstelle JSONs!";

      createJSON($post, $endpoint, $newOrgs);
    }
  }else{
    say "Paketdatei nicht vorhanden!";

    die "Zum Erstellen mit Parameter '--packages' starten!";
  }
}

# No parameters

if(scalar @ARGV == 0 || (!$argJ && !$argP)){

  say STDOUT "Keine Parameter gefunden!";
  say STDOUT "Mögliche Parameter sind:";
  say STDOUT "'--packages \"data_source,username,password\"'";
  say STDOUT "'--json [\"Sigel\"]'";
  say STDOUT "'--endpoint zdb|gvk|natliz'";
  say STDOUT "'--post [\"URL\"]'";
  say STDOUT "'--new_orgs'";
  say STDOUT "'--pub_type journal|book|all'";
}

# Query Sigelverzeichnis via SRU for package metadata

sub getZdbName {
  my $sig = shift;

  print strftime '%Y-%m-%d %H:%M:%S', localtime;
  print " - Sigel: $sig \n";

  my %pkgInfos = (
    'name' => "",
    'type' => "",
    'provider' => "",
    'platform' => "",
    'mainUrl' => "",
    'authority' => "",
    'scope' => ""
  );

  my %attrs = (
      base => 'http://sru.gbv.de/isil',
      query => 'pica.isi='.$sig,
      recordSchema => 'picaxml',
      parser => 'picaxml'
  );
  my $importer = Catmandu::Importer::SRU->new(%attrs)
    or die " - Abfrage über ".$attrs{'base'}." fehlgeschlagen!\n";

  $importer->each(
    sub {
      my $packageInstance = shift;

      if(pica_value($packageInstance, '035Ea') ne 'I'
        && pica_value($packageInstance, '008Hd') eq $sig
      ){
        my $messyName = pica_value($packageInstance, '029Aa');
        my $bracketPos = index($messyName, '[');

        if($bracketPos > 0){
          $pkgInfos{'name'} = substr($messyName, 0, $bracketPos-1);
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
              }elsif($subField eq 'z'){
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

      }else{
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
    WHERE \( meta_type = 'NLLicenceModelStandard'
    OR meta_type = 'NLLicenceModelOptIn' \)
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
      }elsif($licence_type eq 'NLLicenceModelOptIn'){
        $lType = "AL";
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
        }
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

              }elsif($tempISIL =~ /^\d+\/?[\d\w]*/){

                if($tempISIL =~ /^\d+\w*$/){

                }elsif($tempISIL =~ /^\d+\/[\d\w]+$/){

                  $tempISIL =~ s/\//-/g;

                }else{

                  $hasError = 1;

                }
              }else{

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
                say STDOUT "Suche nach ISIL: $tempISIL!";
                my $orgImporter = Catmandu::Importer::SRU->new(%bibAttrs)
                  or die "Abfrage über ".$bibAttrs{'base'}." fehlgeschlagen!\n";

                my $sruOrg = $orgImporter->first();

                if($sruOrg){
                  $pkg{'orgStats'}{'numValidSig'}++;
                  $isil = $tempISIL;
                  $knownIsils{$tempISIL} = 1;
                }else{
                  $pkg{'orgStats'}{'numWrongSig'}++;
                  say "Suche nach $tempISIL erfolglos!";
                  $knownIsils{$tempISIL} = 0;
                }
              }elsif($knownIsils{$tempISIL} == 1){
                $isil = $tempISIL;
              }
            }else{
              $pkg{'orgStats'}{'numWrongSig'}++;
              say "Sigel $orgSigel ist offensichtlich ungültig.";
            }
          }else{
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
          base => 'http://sru.gbv.de/zdbdb',
          query => 'pica.isl='.$pkgSigel,
          recordSchema => 'picaxml',
          _max_results => 1,
          parser => 'picaxml'
      );
      my $titleImporter = Catmandu::Importer::SRU->new(%zdbAttrs)
        or die " - Abfrage über ".$zdbAttrs{'base'}." fehlgeschlagen!\n";
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
    }else{
      say "Kein Paketsigel oder falsches Format in zuid: $zuid.";
    }
  };
  $dbh->disconnect;

  say $out $JSON->pretty(1)->encode( \%alljson );

  close($out);
  sleep 1;
  return 0;
}

# Create packages, tipps and titles as GOKb-JSON (and upload if requested)

my %globalIDs;
my @unknownRelIds;
my %orgsToAdd;
my %allISSN;
my %known;

sub createJSON {

  my $postData = shift;
  my $endpoint = shift;
  my $newOrgs = shift;

  my $json_seals = do {
    open(my $json_fh, '<' , $knownSeals)
        or die("Can't open \$knownSeals\": $!\n");
    local $/;
    <$json_fh>
  };

  # Input JSON handling

  %known = %{decode_json($json_seals)}
    or die "JSON nicht vorhanden!\n";
  my %knownSelection;

  if($filter){
    if($known{$filter}){
      $knownSelection{$filter} = $known{$filter};

      say "Generating JSON only for $filter!";
    }else{
      say "Paket nicht bekannt! Suche Metadaten über Sigelstelle..";

      my %pkgInfos = getZdbName($filter);
      my $lType;

      if(length($pkgInfos{'type'})){
        if($pkgInfos{'type'} =~ /Allianz-Lizenz/){
          $lType = "AL";
        }elsif($pkgInfos{'type'} eq "Nationallizenz"){
          $lType = "NL";
        }else{
          $lType = $pkgInfos{'type'};
        }
        say "Verarbeite Paket vom Typ ".$pkgInfos{'type'}."";
      }else{
        say "Konnte den Pakettyp nicht identifizieren.";
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
      }else{
        say "Zurückgeliefertes Paket hat keinen Namen!";
        return -1;
      }
    }
  }else{
    %knownSelection = %known;

    say "Generating JSON for all packages!";
  }

  $packageDir->mkpath( { verbose => 0 } );
  $titleDir->mkpath( { verbose => 0 } );
  $warningDir->mkpath( { verbose => 0 } );

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

  foreach my $sigel (keys %knownSelection){
  
    say "Processing Package ".($packagesTotal + 1).", ".$sigel."...";
    
    my $pkgScope = $knownSelection{$sigel}{'scope'};
    my $noZdbOrgs = 0;
    
    if($knownSelection{$sigel}{'zdbOrgs'} && scalar @{$knownSelection{$sigel}{'zdbOrgs'}} == 0){
      $noZdbOrgs = 1;
    }
    
    if($requestedType eq "journal" && $pkgScope !~ /E-Journals/){
      say "Keine verknüpften Institutionen in der ZBD bzw. nicht als E-Journal-Paket markiert. Überspringe Paket.";
      next;
    }
    
    my %currentPackage = %{$knownSelection{$sigel}};
    
    my ($package, $pkgStats, $pkgWarn) = processPackage(%currentPackage);

    my %package = %{$package};

    if (scalar @{$package{'tipps'}} == 0){
      say "Paket $sigel hat keine TIPPs und wird nicht angelegt!";
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
      }else{
        $globalStats{$pkgStat} += $pkgStats{$pkgStat};
      }
    }
    
    my $json_pkg = JSON->new->utf8->canonical;
    my $out_pkg;
    my $packageFn = $sigel."_".$endpoint;
    
    if($filter){
      my $pfile = $packageDir->file("$packageFn.json");

      $pfile->touch();
      $out_pkg = $pfile->openw();
    }

    if($filter){
      say $out_pkg $json_pkg->pretty(1)->encode( \%package );
    }

    if($postData == 1){
      sleep 10;
      say "Submitting Package $sigel to GOKb (".$gokbCreds{'base'}.")";

      my $postResult = postData('crossReferencePackage', \%package);

      if($postResult != 0){
        say "Could not Upload Package $sigel! Errorcode $postResult";
        say "Giving it one more try!";
        sleep 10;
        if(postData('crossReferencePackage', \%package) != 0){
          say "Second try failed as well. Adding to report..";
          $skippedPackages .= $sigel." ";
        }
      }
    }
    $packagesTotal++;
  } ## End Package

  # Write collected warnings to file

  say $out_warnings
    $json_warning->pretty(1)->encode( \%authorityNotes );
  say $out_warnings_zdb
    $json_warning_zdb->pretty(1)->encode(\%authorityNotesZDB);
  say $out_warnings_gvk
    $json_warning_gvk->pretty(1)->encode(\%authorityNotesGVK);

  # Write collected titles & orgs to file

  if($filter){
    my $tfileName = "titles_$filter"."_"."$endpoint";
    my $tfile = $titleDir->file("$tfileName.json");
    $tfile->touch();

    my $out_titles = $tfile->openw();

    say $out_titles $json_titles->pretty(1)->encode( \@allTitles );

    close($out_titles);
  }

  my $ofileName = "gnd_orgs_$endpoint";
  my $out_orgs;

  my $orgsFile;

  if(!$filter){
    $orgsFile = $orgsDir->file("$ofileName.json");
  }else{
    my $odir = $orgsDir->subdir($filter);
    $odir->mkpath({verbose => 0});
    $orgsFile = $odir->file("$ofileName.json");
  }

  $orgsFile->touch();

  $out_orgs = $orgsFile->openw();

  say $out_orgs $json_orgs->pretty(1)->encode( \%orgsToAdd );

#   if(scalar @unknownRelIds > 0){
#     my $ufn = "Unknown IDs";
#     if($filter){
#       $ufn .= " $filter";
#     }
#     my $ufile = file($ufn);
#     $ufile->touch();
# 
#     my $out_unknown = $ufile->openw();
# 
#     foreach my $unknownId (@unknownRelIds){
#       if(!$globalIDs{$unknownId}){
#         say $out_unknown $unknownId;
#       }
#     }
#   }

  # Submit new Orgs to GOKb

  my $skippedOrgs = 0;
  my $numNewOrgs = scalar keys %orgsToAdd;

  if($postData == 1 && $newOrgs == 1){
    sleep 3;

    say "Submitting $numNewOrgs Orgs to GOKb (".$gokbCreds{'base'}.")";

    foreach my $org (keys %orgsToAdd){
      my %curOrg = %{ $orgsToAdd{$org} };
      my $postResult = postData('assertOrg', \%curOrg);

      if($postResult != 0){
        say "Could not upload Org! Errorcode $postResult";

        $skippedOrgs++;
      }
    }
  }

  # Submit collected titles to GOKb

  my $skippedTitles = 0;

  if($postData == 1){
    sleep 3;

    my $sumTitles = $globalStats{'titlesTotal'} ? $globalStats{'titlesTotal'} : 0;

    say "Submitting $sumTitles titles to GOKb (".$gokbCreds{'base'}.")";

    foreach my $title (@allTitles){
      my %curTitle = %{ $title };
      my $postResult = postData('crossReferenceTitle', \%curTitle);

      if($postResult != 0){
        say "Could not upload Title! Errorcode $postResult";

        $skippedTitles++;
      }
    }
  }

  ## Final statistics

  my $timeElapsed = duration(time() - $startTime);
  my $finishedRun = strftime '%Y-%m-%d %H:%M:%S', localtime;

  say "\n**********************\n";

  say "Run finished at $finishedRun";
  say "Runtime: $timeElapsed";
  
  foreach my $gStatKey (keys %globalStats){
    say "$gStatKey: ".$globalStats{$gStatKey};
  }
  
#   say $globalStats{'titlesTotal'}." relevante Titel in $packagesTotal Paketen";
#   say $globalStats{'numNoUrl'}." Titel ohne relevante URL";
#   say $globalStats{'duplicateISSNs'}." ZDB-ID-Änderungen ohne ISSN-Anpassung";
#   say $globalStats{'wrongISSN'}." eISSNs als Parallel-ISSN (005P0)";
#   say $globalStats{'noPubGiven'}." Titel ohne Verlag (033An)";
#   say $globalStats{'noPubMatch'}." Verlagsfelder mit der GOKb unbekanntem Verlagsnamen (033An)";
#   say $globalStats{'pubFromGnd'}." Verlage durch GND-Verweis identifiziert (029Ga)";
#   say $globalStats{'pubFromAuthor'}." als Verlag verwendete Autoren (021Ah)";
#   say $globalStats{'pubFromCorp'}." als Verlag verwendete Primärkörperschaften (029Aa)";

  if($skippedPackages ne ""){
    say "Wegen Fehler beim Upload übersprungene Pakete: $skippedPackages";
  }

  if($skippedTitles != 0){
    say "Anzahl wegen Fehler beim Upload übersprungene Titel: $skippedTitles";
  }

  if($skippedOrgs != 0){
    say "Anzahl wegen Fehler beim Upload übersprungene Orgs: $skippedOrgs";
  }

  say "\n**********************\n";
}


  ################ PACKAGE ################
  
  
sub processPackage {
  my %packageInfo = @_;
  my $currentTitle = 0;
  my %allISSN;
  my %inPackageIDs;
  my %package;
  my %packageWarnings;
  
  my %pkgStats = (
    'titlesTotal' => 0,
    'duplicateISSNs' => 0,
    'duplicateZDBids' => 0,
    'noISSN' => 0,
    'wrongISSN' => 0,
    'pubFromGnd' => 0,
    'pubFromAuthor' => 0,
    'pubFromCorp' => 0,
    'numNoUrl' => 0,
    'noPubMatch' => 0,
    'noPubGiven' => 0,
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

  my $provider = $packageInfo{'provider'};
  my $pkgName = $packageInfo{'name'};

  $provider =~ s/Anbieter:\s//;
  $pkgName =~ s/:/ -/g;

  my $pkgYear = strftime '%Y', localtime;

  my $pkgPlatform = "";

  if ( $packageInfo{'platform'} ) {
    $pkgPlatform = $packageInfo{'platform'};
  }else{
    my $pkgUrl = URI->new( $packageInfo{'url'} );
    my $pkgUrlHost;

    if($pkgUrl->has_recognized_scheme){
      $pkgUrlHost = $pkgUrl->authority;

      if ($pkgUrlHost) {
        $pkgPlatform = $pkgUrlHost;
      }
    }
  }

  my %pkgSource;

  my $pkgType = $packageInfo{'type'};

  say "Package Type is: $pkgType, endpoint is $endpoint";

  if($endpoint eq 'gvk' && $pkgType eq "NL"){
    %pkgSource = (
      url => "http://sru.gbv.de/gvk",
      name => "GVK-SRU",
      normname => "GVK_SRU"
    );
  }elsif($endpoint eq 'zdb' || ($endpoint eq 'gvk' && $pkgType eq "AL" )){
    %pkgSource = (
      url => "http://sru.gbv.de/zdbdb",
      name => "ZDB-SRU",
      normname => "ZDB_SRU"
    );
  }elsif($endpoint eq 'natliz'){
    %pkgSource = (
      url => "http://sru.gbv.de/natliz",
      name => "Natliz-SRU",
      normname => "Natliz_SRU"
    );
  }
  my $pkgNoProv = "$pkgName: $pkgType";

  $package{'packageHeader'} = {
    name => ($provider ?  "$provider: " : "")."$pkgName: $pkgType",
    identifiers => [{ type => "isil", value => $packageInfo{'sigel'} }],
    additionalProperties => [],
    variantNames => [$provider ne "" ? $pkgNoProv : ""],
    scope => "",
    listStatus => "In Progress",
    breakable => "No",
    consistent => "Yes",
    fixed => "No",
    paymentType => "",
    global => "Consortium",
    listVerifier => "",
    userListVerifier => $userListVer,
    nominalPlatform => $pkgPlatform,
    nominalProvider => $provider,
    listVerifiedDate => $listVerDate,
    source => \%pkgSource,
    curatoryGroups => ["LAS:eR", "VZG"],
  };

  $package{'tipps'} = [];
  
  my %attrs;
  
  if($endpoint eq "gvk" && $pkgType eq "NL"){
    my $qryString = 'pica.xpr='.$packageInfo{'sigel'};

    if($requestedType eq "journal"){
      $qryString .= ' and (pica.mak=Ob* or pica.mak=Od*)';
    }elsif($requestedType eq "book"){
      $qryString .= ' and pica.mak=Oa*';
    }else{
      $qryString .= ' and pica.mak=O*';
    }

    $qryString .= ' sortBy year/sort.ascending';

    %attrs = (
      base => 'http://sru.gbv.de/gvk',
      query => $qryString,
      recordSchema => 'picaxml',
      parser => 'picaxml',
      _max_results => 5
    );

    my $sruTitles = Catmandu::Importer::SRU->new(%attrs)
      or die "Abfrage über ".$attrs{'base'}." fehlgeschlagen!\n";
    
    while (my $titleRecord = $sruTitles->next){
      $currentTitle++;
      if(pica_value($titleRecord, '006Z0')){
        print STDOUT "Verarbeite Titel ".($currentTitle);
        print STDOUT " von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '006Z0').")\n";
      }else{
        print STDOUT "Verarbeite Titel ".($currentTitle);
        print STDOUT " von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '003@0').")\n";
      }
      
      my %packageHeader = %{$package{'packageHeader'}};
      my (@tipps, %titleStats, %warnings) = processTitle($titleRecord, $pkgType, %packageHeader);
      
      if(scalar @tipps > 0){
        push @{ $package{'tipps'} }, @tipps;
      }
      
      foreach my $statsKey (keys %pkgStats){
        if($titleStats{$statsKey}){
          $pkgStats{$statsKey} += $titleStats{$statsKey};
        }
      }
      
      if($warnings{'id'} ne ""){
        if(scalar @{$warnings{'all'}} > 0){
          $packageWarnings{'all'}{$warnings{'id'}} = $warnings{'all'};
        }
        if(scalar @{$warnings{'zdb'}} > 0){
          $packageWarnings{'zdb'}{$warnings{'id'}} = $warnings{'zdb'};
        }
        if(scalar @{$warnings{'gvk'}} > 0){
          $packageWarnings{'gvk'}{$warnings{'id'}} = $warnings{'gvk'};
        }
      }
    }
    
  }elsif($endpoint eq "zdb" || ($endpoint eq "gvk" && $pkgType eq "AL" )){
    my $qryString = 'pica.isl='.$packageInfo{'sigel'};

    %attrs = (
      base => 'http://sru.gbv.de/zdbdb',
      query => $qryString,
      recordSchema => 'picaxml',
      parser => 'picaxml',
      _max_results => 3
    );
    
    my $sruTitles = Catmandu::Importer::SRU->new(%attrs)
      or die "Abfrage über ".$attrs{'base'}." fehlgeschlagen!\n";
      
    while (my $titleRecord = $sruTitles->next){
      $currentTitle++;
      if(pica_value($titleRecord, '006Z0')){
        print STDOUT "Verarbeite Titel ".($currentTitle);
        print STDOUT " von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '006Z0').")\n";
      }else{
        print STDOUT "Verarbeite Titel ".($currentTitle);
        print STDOUT " von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '003@0').")\n";
      }

      my %packageHeader = %{$package{'packageHeader'}};
      my ($tipps, $titleStats, $titleWarnings) = processTitle($titleRecord, $pkgType, %packageHeader);
      
      my @tipps = @{$tipps};
      my %titleStats = %{$titleStats};
      my %titleWarnings = %{$titleWarnings};
      
      if(scalar @tipps > 0){
        push @{ $package{'tipps'} }, @tipps;
      }
      
      foreach my $statsKey (keys %pkgStats){
        if($titleStats{$statsKey}){
          $pkgStats{$statsKey} += $titleStats{$statsKey};
        }
      }
      
      if($titleWarnings{'id'} ne ""){
        if(scalar @{$titleWarnings{'all'}} > 0){
          $packageWarnings{'all'}{$titleWarnings{'id'}} = $titleWarnings{'all'};
        }
        if(scalar @{$titleWarnings{'zdb'}} > 0){
          $packageWarnings{'zdb'}{$titleWarnings{'id'}} = $titleWarnings{'zdb'};
        }
        if(scalar @{$titleWarnings{'gvk'}} > 0){
          $packageWarnings{'gvk'}{$titleWarnings{'id'}} = $titleWarnings{'gvk'};
        }
      }
    }
    
    if($requestedType eq "all"){
    
      my $qryString = 'pica.xpr='.$packageInfo{'sigel'}." and pica.mak=Oa*";
    
      %attrs = (
        base => 'http://sru.gbv.de/gvk',
        query => $qryString,
        recordSchema => 'picaxml',
        parser => 'picaxml',
        _max_results => 5
      );
      
      my $sruBooks = Catmandu::Importer::SRU->new(%attrs)
        or die "Abfrage über ".$attrs{'base'}." fehlgeschlagen!\n";
        
      while (my $titleRecord = $sruBooks->next){
        $currentTitle++;
        if(pica_value($titleRecord, '006Z0')){
          print STDOUT "Verarbeite Titel ".($currentTitle);
          print STDOUT " von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '006Z0').")\n";
        }else{
          print STDOUT "Verarbeite Titel ".($currentTitle);
          print STDOUT " von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '003@0').")\n";
        }

        my %packageHeader = %{$package{'packageHeader'}};
        my ($tipps, $titleStats, $titleWarnings) = processTitle($titleRecord, $pkgType, %packageHeader);
        
        my @tipps = @{$tipps};
        my %titleStats = %{$titleStats};
        my %titleWarnings = %{$titleWarnings};
        
        if(scalar @tipps > 0){
          push @{ $package{'tipps'} }, @tipps;
        }
        
        foreach my $statsKey (keys %pkgStats){
          if($titleStats{$statsKey}){
            $pkgStats{$statsKey} += $titleStats{$statsKey};
          }
        }
        
        if($titleWarnings{'id'} ne ""){
          if(scalar @{$titleWarnings{'all'}} > 0){
            $packageWarnings{'all'}{$titleWarnings{'id'}} = $titleWarnings{'all'};
          }
          if(scalar @{$titleWarnings{'zdb'}} > 0){
            $packageWarnings{'zdb'}{$titleWarnings{'id'}} = $titleWarnings{'zdb'};
          }
          if(scalar @{$titleWarnings{'gvk'}} > 0){
            $packageWarnings{'gvk'}{$titleWarnings{'id'}} = $titleWarnings{'gvk'};
          }
        }
      }
    }
      
  }elsif($endpoint eq "natliz"){
    my $qryString = 'pica.xpr='.$packageInfo{'sigel'};
    
    %attrs = (
      base => 'http://sru.gbv.de/natliz',
      query => $qryString,
      recordSchema => 'picaxml',
      parser => 'picaxml',
      _max_results => 5
    );
    
    if ($requestedType eq 'book'){
    
      my $sruTitles = Catmandu::Importer::SRU->new(%attrs)
        or die "Abfrage über ".$attrs{'base'}." fehlgeschlagen!\n";

      while (my $titleRecord = $sruTitles->next){
        $currentTitle++;
        if(pica_value($titleRecord, '006Z0')){
          print STDOUT "Verarbeite Titel ".($currentTitle);
          print STDOUT " von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '006Z0').")\n";
        }else{
          print STDOUT "Verarbeite Titel ".($currentTitle);
          print STDOUT " von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '003@0').")\n";
        }
        
        my %packageHeader = %{$package{'packageHeader'}};
        my (@tipps, %titleStats, %warnings) = processTitle($titleRecord, $pkgType, %packageHeader);
        
        if(scalar @tipps > 0){
          push @{ $package{'tipps'} }, @tipps;
        }
        
        foreach my $statsKey (keys %pkgStats){
          if($titleStats{$statsKey}){
            $pkgStats{$statsKey} += $titleStats{$statsKey};
          }
        }
        
        if($warnings{'id'} ne ""){
          if(scalar @{$warnings{'all'}} > 0){
            $packageWarnings{'all'}{$warnings{'id'}} = $warnings{'all'};
          }
          if(scalar @{$warnings{'zdb'}} > 0){
            $packageWarnings{'zdb'}{$warnings{'id'}} = $warnings{'zdb'};
          }
          if(scalar @{$warnings{'gvk'}} > 0){
            $packageWarnings{'gvk'}{$warnings{'id'}} = $warnings{'gvk'};
          }
        }
      }
        
    }elsif($requestedType eq 'journal'){

      $attrs{'base'} = 'http://sru.gbv.de/natlizzss';
    
      my $sruTitles = Catmandu::Importer::SRU->new(%attrs)
        or die "Abfrage über ".$attrs{'base'}." fehlgeschlagen!\n";
        
      while (my $titleRecord = $sruTitles->next){
        $currentTitle++;
        if(pica_value($titleRecord, '006Z0')){
          print STDOUT "Verarbeite Titel ".($currentTitle);
          print STDOUT " von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '006Z0').")\n";
        }else{
          print STDOUT "Verarbeite Titel ".($currentTitle);
          print STDOUT " von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '003@0').")\n";
        }
        
        my %packageHeader = %{$package{'packageHeader'}};
        my (@tipps, %titleStats, %warnings) = processTitle($titleRecord, $pkgType, %packageHeader);
        
        if(scalar @tipps > 0){
          push @{ $package{'tipps'} }, @tipps;
        }
        
        foreach my $statsKey (keys %pkgStats){
          if($titleStats{$statsKey}){
            $pkgStats{$statsKey} += $titleStats{$statsKey};
          }
        }

        if($warnings{'id'} ne ""){
          if(scalar @{$warnings{'all'}} > 0){
            $packageWarnings{'all'}{$warnings{'id'}} = $warnings{'all'};
          }
          if(scalar @{$warnings{'zdb'}} > 0){
            $packageWarnings{'zdb'}{$warnings{'id'}} = $warnings{'zdb'};
          }
          if(scalar @{$warnings{'gvk'}} > 0){
            $packageWarnings{'gvk'}{$warnings{'id'}} = $warnings{'gvk'};
          }
        }
      }
        
    }elsif($requestedType eq 'all'){
      
      my $sruBooks = Catmandu::Importer::SRU->new(%attrs)
        or die "Abfrage über ".$attrs{'base'}." fehlgeschlagen!\n";
        
      say STDOUT "SRU-Antwort für Monographien erhalten!";
        
      while (my $titleRecord = $sruBooks->next){
        $currentTitle++;
        if(pica_value($titleRecord, '006Z0')){
          print STDOUT "Verarbeite Titel ".($currentTitle);
          print STDOUT " von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '006Z0').")\n";
        }else{
          print STDOUT "Verarbeite Titel ".($currentTitle);
          print STDOUT " von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '003@0').")\n";
        }
        
        my %packageHeader = %{$package{'packageHeader'}};
        my ($btipps, $btitleStats, $btitleWarnings) = processTitle($titleRecord, $pkgType, %packageHeader);
        my @btipps = @{$btipps};
        my %btitleStats = %{$btitleStats};
        my %btitleWarnings = %{$btitleWarnings};
        
        if(scalar @btipps > 0){
          push @{ $package{'tipps'} }, @btipps;
        }
        
        foreach my $statsKey (keys %pkgStats){
          if($btitleStats{$statsKey}){
            $pkgStats{$statsKey} += $btitleStats{$statsKey};
          }
        }
        
        if($btitleWarnings{'id'} ne ""){
          if(scalar @{$btitleWarnings{'all'}} > 0){
            $packageWarnings{'all'}{$btitleWarnings{'id'}} = $btitleWarnings{'all'};
          }
          if(scalar @{$btitleWarnings{'zdb'}} > 0){
            $packageWarnings{'zdb'}{$btitleWarnings{'id'}} = $btitleWarnings{'zdb'};
          }
          if(scalar @{$btitleWarnings{'gvk'}} > 0){
            $packageWarnings{'gvk'}{$btitleWarnings{'id'}} = $btitleWarnings{'gvk'};
          }
        }
      }

      $attrs{'base'} = 'http://sru.gbv.de/natlizzss';
        
      my $sruJournals = Catmandu::Importer::SRU->new(%attrs)
        or die "Abfrage über ".$attrs{'base'}." fehlgeschlagen!\n";
        
      say STDOUT "SRU-Antwort für Journals erhalten!";
        
      while (my $titleRecord = $sruJournals->next){
        $currentTitle++;
        if(pica_value($titleRecord, '006Z0')){
          print STDOUT "Verarbeite Titel ".($currentTitle);
          print STDOUT " von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '006Z0').")\n";
        }else{
          print STDOUT "Verarbeite Titel ".($currentTitle);
          print STDOUT " von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '003@0').")\n";
        }
        
        my %packageHeader = %{$package{'packageHeader'}};
        my ($jtipps, $jtitleStats, $jtitleWarnings) = processTitle($titleRecord, $pkgType, %packageHeader);
        my @jtipps = @{$jtipps};
        my %jtitleStats = %{$jtitleStats};
        my %jtitleWarnings = %{$jtitleWarnings};
        
        if(scalar @jtipps > 0){
          push @{ $package{'tipps'} }, @jtipps;
        }
        
        foreach my $statsKey (keys %pkgStats){
          if($jtitleStats{$statsKey}){
            $pkgStats{$statsKey} += $jtitleStats{$statsKey};
          }
        }
        
        if($jtitleWarnings{'id'} ne ""){
          if(scalar @{$jtitleWarnings{'all'}} > 0){
            $packageWarnings{'all'}{$jtitleWarnings{'id'}} = $jtitleWarnings{'all'};
          }
          if(scalar @{$jtitleWarnings{'zdb'}} > 0){
            $packageWarnings{'zdb'}{$jtitleWarnings{'id'}} = $jtitleWarnings{'zdb'};
          }
          if(scalar @{$jtitleWarnings{'gvk'}} > 0){
            $packageWarnings{'gvk'}{$jtitleWarnings{'id'}} = $jtitleWarnings{'gvk'};
          }
        }
      }
        
      $attrs{'base'} = 'http://sru.gbv.de/natlizfak';
        
      my $sruDatabases = Catmandu::Importer::SRU->new(%attrs)
        or die "Abfrage über ".$attrs{'base'}." fehlgeschlagen!\n";
        
      say STDOUT "SRU-Antwort für Datenbanken erhalten!";
        
      while (my $titleRecord = $sruDatabases->next){
        $currentTitle++;
        if(pica_value($titleRecord, '006Z0')){
          print STDOUT "Verarbeite Titel ".($currentTitle);
          print STDOUT " von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '006Z0').")\n";
        }else{
          print STDOUT "Verarbeite Titel ".($currentTitle);
          print STDOUT " von Paket ".$packageInfo{'sigel'}." (".pica_value($titleRecord, '003@0').")\n";
        }
      
        my %packageHeader = %{$package{'packageHeader'}};
        my ($dtipps, $dtitleStats, $dtitleWarnings) = processTitle($titleRecord, $pkgType, %packageHeader);
        my @dtipps = @{$dtipps};
        my %dtitleStats = %{$dtitleStats};
        my %dtitleWarnings = %{$dtitleWarnings};
        
        if(scalar @dtipps > 0){
          push @{ $package{'tipps'} }, @dtipps;
        }
        
        foreach my $statsKey (keys %pkgStats){
          if($dtitleStats{$statsKey}){
            $pkgStats{$statsKey} += $dtitleStats{$statsKey};
          }
        }
        
        if($dtitleWarnings{'id'} ne ""){
          if(scalar @{$dtitleWarnings{'all'}} > 0){
            $packageWarnings{'all'}{$dtitleWarnings{'id'}} = $dtitleWarnings{'all'};
          }
          if(scalar @{$dtitleWarnings{'zdb'}} > 0){
            $packageWarnings{'zdb'}{$dtitleWarnings{'id'}} = $dtitleWarnings{'zdb'};
          }
          if(scalar @{$dtitleWarnings{'gvk'}} > 0){
            $packageWarnings{'gvk'}{$dtitleWarnings{'id'}} = $dtitleWarnings{'gvk'};
          }
        }
      }
    }
  }
  return \%package, \%pkgStats, \%packageWarnings;
  say "Finished processing $currentTitle Titles of package ".$packageInfo{'sigel'};
}

    ################ TITLEINSTANCE ################

sub processTitle {
  my ($titleRecord, $pkgType, %pkgInfo) = @_;
  my $materialType = pica_value($titleRecord, '002@0');
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

  my %titleWarnings = (
    'id' => "",
    'all' => [],
    'gvk' => [],
    'zdb' => []
  );

    # Process material code

  if($isJournal){
    $gokbType = "Serial";
    $gokbMedium = "Journal";
  }elsif($typeChar eq 'a'){
    $gokbType = "Monograph";
    $gokbMedium = "Book";
  }else{
    say "Kann Materialtyp für $ppn nicht bestimmen...";
    return \@tipps, \%titleStats, \%titleWarnings;
  }

    # -------------------- Identifiers --------------------

  $titleInfo{'identifiers'} = [];

  ## PPN
  if($endpoint eq "gvk" && $pkgType eq "NL"){
    push @{ $titleInfo{'identifiers'} } , {
      'type' => "gvk_ppn",
      'value' => $ppn
    };
  }elsif( $endpoint eq "zdb" || ($endpoint eq "gvk" && $pkgType eq "AL") ){
    push @{ $titleInfo{'identifiers'} } , {
      'type' => "zdb_ppn",
      'value' => $ppn
    };
  }elsif($endpoint eq "natliz"){
    push @{ $titleInfo{'identifiers'} } , {
      'type' => "natliz_ppn",
      'value' => $ppn
    };
  }

  ## DOI

  my $doi;

  if(($endpoint eq "gvk" && $pkgType eq "NL") || $endpoint eq "natliz"){
    $doi = pica_value($titleRecord, '004V0');
  }else{
    $doi = pica_value($titleRecord, '004P0');
  }

  if($doi){
    push @{ $titleInfo{'identifiers'} } , {
      'type' => "doi",
      'value' => $doi
    };
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
    }else{
      print "Titel mit ppn ".pica_value($titleRecord, '003@0');
      print " hat keine ZDB-ID! Überspringe Titel..\n";

      push @{ $titleWarnings{'all'} }, {
          '006Z0' => 'Keine ZDB-ID angegeben!'
      };

      push @{ $titleWarnings{'gvk'} }, {
          '006Z0' => 'Keine ZDB-ID angegeben!'
      };
      $id = pica_value($titleRecord, '003@0');

      return \@tipps, \%titleStats, \%titleWarnings;
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
            }elsif($globalIDs{$id} && none {$_ eq $eissn} @globalIssns){
              say "eISSN $eissn kommt in bereits erschlossenem Titel $id nicht vor!";

              push @{ $titleWarnings{'all'} }, {
                '005A0' => $eissn,
                'comment' => 'ISSN bei gleicher ZDB-ID nicht vergeben?'
              };

              push @{ $titleWarnings{'zdb'} }, {
                '005A0' => $eissn,
                'comment' => 'ISSN bei gleicher ZDB-ID nicht vergeben?'
              };
            }else{
              push @eissn, $eissn;

              push @{ $titleInfo{'identifiers'}} , {
                'type' => "eissn",
                'value' => $eissn
              };

#               if($allISSN{$eissn}){
#                 say "eISSN $eissn in Titel $id wurde bereits vergeben!";
# 
#                 $titleStats{'duplicateISSNs'} ? $titleStats{'duplicateISSNs'}++ : $titleStats{'duplicateISSNs'} = 1;
# 
#                 push @{ $titleWarnings{'all'} }, {
#                   '005A0' => $eissn,
#                   'comment' => 'gleiche eISSN nach Titeländerung?'
#                 };
# 
#                 push @{ $titleWarnings{'zdb'} }, {
#                   '005A0' => $eissn,
#                   'comment' => 'gleiche eISSN nach Titeländerung?'
#                 };
#               }

#               $allISSN{$eissn} = pica_value($titleRecord, '021Aa');
            }
          }
          $subfPos++;
        }
      }
    }else{
      if($titleStats{'noISSN'}){
        $titleStats{'noISSN'}++;
      }else{
        $titleStats{'noISSN'} = 1;
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
            }else{

              if($allISSN{$pissn}){
                print "Parallel-ISSN $pissn";
                print " in Titel $id wurde bereits als eISSN vergeben!\n";

                if($titleStats{'wrongISSN'}){
                  $titleStats{'wrongISSN'}++;
                }else{ 
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

  }elsif($gokbMedium eq "Book"){
    $id = $ppn;

    if(pica_value($titleRecord, '004AA')){
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
            }else{
              say "Mehrere ISBNs in einem PICA-Feld!";
            }
          }
          if($subField eq 'f'){
            $isbnType = $isbnValue[$subfPos+1];
          }
          $subfPos++;
        }
        if(!$isbnType || $isbnType eq 'Online'){
          push @{ $titleInfo{'identifiers'}} , {
            'type' => "isbn",
            'value' => $isbn
          };
        }
      }
    }
  }else{
    say "Kann Materialtyp für $ppn nicht bestimmen...";
    return \@tipps, \%titleStats, \%titleWarnings;
  }
  $titleWarnings{'id'} = $id;
  ## Andere Identifier, z.B. OCLC-No.

#       my @otherIdentifiers = @{ pica_fields($titleRecord, '006X') };
#
#       if(scalar @otherIdentifiers > 0){
#         foreach my $otherID (@otherIdentifiers){
#           my @otherID = @{ $otherID };
#           my $subfPos = 0;
#           foreach my $subField (@otherID){
#             if($subField eq 'c'){
#               push @{ $titleInfo{'identifiers'}} , {
#                   'type' => $otherID[$subfPos+1],
#                   'value' => $otherID[$subfPos+2] eq '0'
#                     ? $otherID[$subfPos+3]
#                     : ""
#               };
#             }
#             $subfPos++;
#           }
#         }
#       }

  # Check, if the title is a journal
  # (shouldn't be necessary since it should be included in the search query)

  if(($requestedType eq "journal" && !$isJournal) || ($requestedType eq "book" && $isJournal)){
    print "Überspringe Titel ".pica_value($titleRecord, '021Aa');
    print ", Materialcode: $materialType\n";

    return \@tipps, \%titleStats, \%titleWarnings;
  }

  # -------------------- Title --------------------

  if(pica_value($titleRecord, '025@a')){
    my $titleField = pica_value($titleRecord, '025@a');

    if($titleField =~ /@/){
      $titleField =~ s/@//;
      say STDOUT "Removed \@ from Title!";
    }
    $titleInfo{'name'} = $titleField;
    
  }elsif(pica_value($titleRecord, '021Aa')){
    my $titleField = pica_value($titleRecord, '021Aa');

    if($titleField =~ /@/){
      $titleField =~ s/@//;
    }
    $titleInfo{'name'} = $titleField;

    if(pica_value($titleRecord, '021Ad') && $typeChar eq 'a'){
      $titleInfo{'name'} .= " - ".pica_value($titleRecord, '021Ad');
    }
    
  }else{
    say "Keinen Titel für ".$ppn." erkannt, überspringe Titel!";

    push @{ $titleWarnings{'all'} }, {
        '021Aa' => pica_value($titleRecord, '021Aa'),
        'comment' => "Kein Titel gefunden!"
    };
    push @{ $titleWarnings{'zdb'} }, {
        '021Aa' => pica_value($titleRecord, '021Aa'),
        'comment' => "Kein Titel gefunden!"
    };

    return \@tipps, \%titleStats, \%titleWarnings;
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
        }else{
          if($releaseEnd{'year'} ne ""){
            $releaseEnd{'year'} = "";
            $releaseEnd{'month'} = "";
            $releaseEnd{'day'} = "";
            $releaseEnd{'volume'} = "";
            $releaseEnd{'issue'} = "";
          }
        }
      }elsif($subField eq 'c' && $releaseStart{'month'} ne ""){
        $releaseStart{'month'} = $releaseNote[$subfPos+1];
      }elsif($subField eq 'b' && $releaseStart{'day'} ne ""){
        $releaseStart{'day'} = $releaseNote[$subfPos+1];
      }elsif($subField eq 'd' && $releaseStart{'volume'} ne ""){
        $releaseStart{'volume'} = $releaseNote[$subfPos+1];
      }elsif($subField eq 'e' && $releaseStart{'issue'} ne ""){
        $releaseStart{'issue'} = $releaseNote[$subfPos+1];
      }elsif($subField eq 'k'){
        $releaseEnd{'year'} = substr($releaseNote[$subfPos+1],0,4);
      }elsif($subField eq 'm' && !$releaseEnd{'month'}){
        $releaseEnd{'month'} = $releaseNote[$subfPos+1];
      }elsif($subField eq 'l' && !$releaseEnd{'day'}){
        $releaseEnd{'day'} = $releaseNote[$subfPos+1];
      }elsif($subField eq 'n' && !$releaseEnd{'volume'}){
        $releaseEnd{'volume'} = $releaseNote[$subfPos+1];
      }elsif($subField eq 'o' && !$releaseEnd{'issue'}){
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
  }elsif($releaseStart{'year'} ne ""){
    $start_year = $releaseStart{'year'};
  }

  if(pica_value($titleRecord, '011@b')){
    if($start_year != 0
      && pica_value($titleRecord, '011@b') >= $start_year
    ){
      $end_year = pica_value($titleRecord, '011@b');
    }
  }elsif($releaseEnd{'year'} ne ""){
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
    'sj' => $start_year,
    'sm' => $start_month,
    'sd' => $start_day,
    'ej' => $end_year,
    'em' => $end_month,
    'ed' => $end_day
  );

  my @dts = transformDate(\%dates);
  if($isJournal){
    $titleInfo{'publishedFrom'} = convertToTimeStamp($dts[0][0], 0);

    $titleInfo{'publishedTo'} = convertToTimeStamp($dts[0][1], 1);
  }else{
    $titleInfo{'dateFirstOnline'} = convertToTimeStamp($dts[0][0], 0)
  }

  # -------------------- Publisher --------------------

  $titleInfo{'publisher_history'} = [];
  
  my @possiblePubs = @{ pica_fields($titleRecord, '033A') };
  my $checkPubs = pica_value($titleRecord, '033An');
  my @altPubs = @{ pica_fields($titleRecord, '033B') };
  
  push(@possiblePubs, @altPubs);
  my @gndPubs;
  
  if($endpoint eq "gvk" && $pkgType eq "NL" ){
    @gndPubs = @{ pica_fields($titleRecord, '029G') };
  }elsif($endpoint eq "zdb" || $pkgType eq "AL"){
    @gndPubs = @{ pica_fields($titleRecord, '029A') };
  }
  
  my $authorField = pica_value($titleRecord, '021Ah');
  my $titleCorpField = pica_value($titleRecord, '021Ae');
  my $corpField = pica_value($titleRecord, '029Aa');

  if(!$checkPubs){
    if($titleStats{'noPubGiven'}){
      $titleStats{'noPubGiven'}++;
    }else{
      $titleStats{'noPubGiven'} = 1;
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
            if($endpoint eq "gvk" && $pkgType eq "NL"){
              push @{ $titleWarnings{'gvk'} }, {
                '033(A/B)' => \@pub,
                'comment' => "Mehrere Verlage in einem PICA-Feld!"
              };
            }elsif($endpoint eq "zdb" || $pkgType eq "AL"){
              push @{ $titleWarnings{'zdb'} }, {
                '033(A/B)' => \@pub,
                'comment' => "Mehrere Verlage in einem PICA-Feld!"
              };
            }
          }
          $preCorrectedPub = $pub[$subfPos+1];
          $tempPub = $pub[$subfPos+1];
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
        }else{ 
          $titleStats{'correctedAbbrs'} = 1;
        }
      }

      if($tempPub =~ /(^|\s)[Aa]ssoc\.?(\s|$)/){

        $tempPub =~ s/(^|\s)([Aa]ssoc)\.?(\s|$)/$1Association$3/g;
        if($titleStats{'correctedAbbrs'}){
          $titleStats{'correctedAbbrs'}++;
        }else{ 
          $titleStats{'correctedAbbrs'} = 1;
        }
      }

      if($tempPub =~ /(^|\s)[Ss]oc\.?(\s|$)/){

        $tempPub =~ s/(^|\s)([Ss]oc)\.?(\s|$)/$1Society$3/g;
        if($titleStats{'correctedAbbrs'}){
          $titleStats{'correctedAbbrs'}++;
        }else{ 
          $titleStats{'correctedAbbrs'} = 1;
        }
      }

      if($tempPub =~ /(^|\s)[Uu]niv\.?(\s|$)/){

        $tempPub =~ s/(^|\s)([Uu]niv)\.?(\s|$)/$1University$3/g;
        if($titleStats{'correctedAbbrs'}){
          $titleStats{'correctedAbbrs'}++;
        }else{ 
          $titleStats{'correctedAbbrs'} = 1;
        }
      }

      if($tempPub =~ /(^|\s)[Aa]cad\.?(\s$)/){

        $tempPub =~ s/(^|\s)([Aa]cad)\.?(\s|$)/$1Academic$3/g;
        if($titleStats{'correctedAbbrs'}){
          $titleStats{'correctedAbbrs'}++;
        }else{ 
          $titleStats{'correctedAbbrs'} = 1;
        }
      }

      if($tempPub =~ /(^|\s)[Vv]erl\.?(\s$)/){

        $tempPub =~ s/(^|\s)([Vv]erl)\.?(\s|$)/$1Verlag$3/g;
        if($titleStats{'correctedAbbrs'}){
          $titleStats{'correctedAbbrs'}++;
        }else{ 
          $titleStats{'correctedAbbrs'} = 1;
        }
      }

      if($tempPub =~ /(^|\s)[Aa]kad\.?(\s$)/){

        $tempPub =~ s/(^|\s)([Aa]kad)\.?(\s|$)/$1Akademie$3/g;
        if($titleStats{'correctedAbbrs'}){
          $titleStats{'correctedAbbrs'}++;
        }else{ 
          $titleStats{'correctedAbbrs'} = 1;
        }
      }

      if($tempPub =~ /(^|\s)[Vv]erb\.?(\s$)/){

        $tempPub =~ s/(^|\s)([Vv]erb)\.?(\s|$)/$1Verband$3/g;
        if($titleStats{'correctedAbbrs'}){
          $titleStats{'correctedAbbrs'}++;
        }else{ 
          $titleStats{'correctedAbbrs'} = 1;
        }
      }

      ## Verlag verifizieren & hinzufügen

      my $ncsuPub = searchNcsuOrgs($tempPub);

      if($ncsuPub){
        push @{ $titleInfo{'publisher_history'}} , {
            'name' => $ncsuPub,
            'startDate' => $pubStart ? $pubStart : "",
            'endDate' => $pubEnd ? $pubEnd : "",
            'status' => "Active"
        };
      }elsif(!$ncsuPub
        || $tempPub =~ /[\[\]]/
        || $tempPub =~ /u\.\s?a\./
      ){
        if($titleStats{'noPubMatch'}){
          $titleStats{'noPubMatch'}++;
        }else{
          $titleStats{'noPubMatch'} = 1;
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

  ## Im Autor- bzw. Körperschaftsfeld nach Ersatz suchen

  if(scalar @{ $titleInfo{'publisher_history'} } == 0) {

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
            $ncsuPub = searchNcsuOrgs($pubName);

            if(!$ncsuPub){
              if($endpoint eq 'zdb'){
                push @{ $titleWarnings{'all'} }, {
                  '029Aa' => $pubName,
                  'comment' => "GND-Org ist nicht in der GOKb vorhanden."
                };
              }else{
                push @{ $titleWarnings{'all'} }, {
                  '029Ga' => $pubName,
                  'comment' => "GND-Org ist nicht in der GOKb vorhanden."
                };
              }
            }
          }elsif($subField eq 'M' || $subField eq '7'){
            $authType = substr($pub[$subfPos+1],0,2);
          }elsif($subField eq '0'){
            $gndID = $pub[$subfPos+1];
          }elsif($subField eq 'b'){
            $branch = $pub[$subfPos+1];
            $isParent = 1;
          }
          $subfPos++;
        }
        if($isParent == 1){
          say "$id - Parent: $pubName, Child: $branch";

          if($endpoint eq 'zdb'){
            push @{ $titleWarnings{'all'} }, {
              '029A' => \@pub,
              'comment' => "GND-Org ist nicht eigenständig."
            };
          }else{
            push @{ $titleWarnings{'all'} }, {
              '029G' => \@pub,
              'comment' => "GND-Org ist nicht eigenständig."
            };
          }
          next;
        }

        if($authType && $authType =~ /Tb/ && $gndID){

          my $orgURI = "http://d-nb.info/".$gndID;

          my %orgObj = (
            'name' => $pubName,
            'identifiers' => [{'type' => "global", 'value' => $orgURI}]
          );

          if(!$orgsToAdd{$pubName}){
            $orgsToAdd{$pubName} = \%orgObj;
          }

          push @{ $titleInfo{'publisher_history'}} , {
              'name' => $pubName,
              'startDate' => "",
              'endDate' => "",
              'status' => "Active"
          };
          
          if($titleStats{'pubFromGnd'}){
            $titleStats{'pubFromGnd'}++;
          }else{
            $titleStats{'pubFromGnd'} = 1;
          }
        }
      }
    }else{
      if($titleCorpField){
        my $ncsuAuthor = searchNcsuOrgs($titleCorpField);

        if($ncsuAuthor){
          push @{ $titleInfo{'publisher_history'}} , {
              'name' => $ncsuAuthor,
              'startDate' => convertToTimeStamp($dts[0][0], 0),
              'endDate' => convertToTimeStamp($dts[0][1], 1),
              'status' => ""
          };
          
          if($titleStats{'pubFromCorp'}){
            $titleStats{'pubFromCorp'}++;
          }else{
            $titleStats{'pubFromCorp'} = 1;
          }
        }
        # print "Used author $authorField as publisher.\n";
      }elsif($authorField){
        my $ncsuAuthor = searchNcsuOrgs($authorField);

        if($ncsuAuthor){
          push @{ $titleInfo{'publisher_history'}} , {
              'name' => $ncsuAuthor,
              'startDate' => convertToTimeStamp($dts[0][0], 0),
              'endDate' => convertToTimeStamp($dts[0][1], 1),
              'status' => ""
          };
          
          if($titleStats{'pubFromAuthor'}){
            $titleStats{'pubFromAuthor'}++;
          }else{
            $titleStats{'pubFromAuthor'} = 1;
          }
        }
        # print "Used author $authorField as publisher.\n";
      }elsif($corpField){
        my $ncsuCorp = searchNcsuOrgs($corpField);

        if($ncsuCorp){
          push @{ $titleInfo{'publisher_history'}} , {
              'name' => $ncsuCorp,
              'startDate' => convertToTimeStamp($dts[0][0], 0),
              'endDate' => convertToTimeStamp($dts[0][1], 1),
              'status' => ""
          };
          if($titleStats{'pubFromCorp'}){
            $titleStats{'pubFromCorp'}++;
          }else{
            $titleStats{'pubFromCorp'} = 1;
          }
        }
      }
      # print "Used corp $corpField as publisher.\n";
    }
  }

  # -------------------- Related titles --------------------

  my @relatedTitles = @{ pica_fields($titleRecord, '039E') };

  foreach my $relatedTitle (@relatedTitles){
    my @relTitle = @{ $relatedTitle };
    my $relationType;
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
      if($endpoint eq "gvk" && $pkgType eq "NL"){
        if($subField eq 'c'){

          $relationType = $relTitle[$subfPos+1];

        }elsif($subField eq 'ZDB' && $relTitle[$subfPos+1] eq '6'){
          my $oID = formatZdbId($relTitle[$subfPos+2]);

          if($oID){
            $relatedID = $oID;
          }
        }elsif($subField eq 't' || $subField eq 'a'){

          $relName = $relTitle[$subfPos+1];

        }elsif($subField eq 'f' || $subField eq 'd'){
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
              }else{
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
              }else{
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
      }elsif($endpoint eq "zdb" || $pkgType eq "AL"){
        if($subField eq 'b'){
          $relationType = $relTitle[$subfPos+1];
        }

        if($subField eq '0'){
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
          }else{
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
      }else{
        $titleStats{'possibleRelations'} = 1;
      }

      say STDOUT "Found possible relation $relatedID";

      my %relAttrs;

      if($endpoint eq "gvk" && $pkgType eq "NL"){
        my $relQryString = 'pica.zdb='.$relatedID;

        if($requestedType eq "journal"){
          $relQryString .= ' and (pica.mak=Ob* or pica.mak=Od*)';
        }

        %relAttrs = (
          base => 'http://sru.gbv.de/gvk',
          query => $relQryString,
          recordSchema => 'picaxml',
          parser => 'picaxml',
          _max_results => 1
        );
      }elsif($endpoint eq "zdb" || $pkgType eq "AL"){
        my $relQryString = 'pica.yyy='.$relatedID;

        %relAttrs = (
          base => 'http://sru.gbv.de/zdbdb',
          query => $relQryString,
          recordSchema => 'picaxml',
          parser => 'picaxml',
          _max_results => 1
        );
      }elsif($endpoint eq "natliz"){
        my $relQryString = 'pica.zdb='.$relatedID;

        %relAttrs = (
          base => 'http://sru.gbv.de/natlizzss',
          query => $relQryString,
          recordSchema => 'picaxml',
          parser => 'picaxml',
          _max_results => 1
        );
      }
      my $relRecord;

      eval{
        my $sruRel = Catmandu::Importer::SRU->new(%relAttrs)
          or warn "Abfrage über ".$relAttrs{'base'}." fehlgeschlagen!\n";

        $relRecord = $sruRel->first();
      }; warn $@ if $@;

      if($relRecord && ref($relRecord) eq 'HASH'){
        $relPPN = pica_value($relRecord, '003@0');

        if($endpoint eq "gvk" && $pkgType eq "NL"){
          if(pica_value($relRecord, '008E')){
            my @relISIL = pica_values($relRecord, '008E');

            foreach my $relISIL (@relISIL){
              if($known{$relISIL}){
                $relIsNl = 1;
              }
            }
          }
          push @{ $relObj{'identifiers'} } , {
            'type' => "gvk_ppn",
            'value' => $relPPN
          };
        }elsif($endpoint eq "zdb" || $pkgType eq "AL"){
          if(pica_value($relRecord, '017B')){
            my @relISIL = pica_values($relRecord, '017B');

            foreach my $relISIL (@relISIL){
              if( $known{$relISIL} || ($filter && $relISIL eq $filter) ){
                $relIsNl = 1;
              }
            }
          }
          push @{ $relObj{'identifiers'} } , {
            'type' => "zdb_ppn",
            'value' => $relPPN
          };
        }
        if(pica_value($relRecord, '039E')){
          my @relRelatedTitles = @{ pica_fields($relRecord, '039E') };

          foreach my $relRelatedTitle (@relRelatedTitles){
            my @rt = @{ $relRelatedTitle };
            my $rSubfPos = 0;

            foreach my $subField (@rt){
              if($endpoint eq "gvk" && $pkgType eq "NL"){
                if($subField eq 'ZDB' && $rt[$rSubfPos+1] eq '6'){
                  my $rID = formatZdbId($rt[$rSubfPos+2]);

                  if($rID && $rID eq $id){
                    $isDirectRelation = 1;
                  }
                }
              }elsif($endpoint eq "zdb" || $pkgType eq "AL"){
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
          }elsif(pica_value($relRecord, '021Aa')){
            $relName = pica_value($relRecord, '021Aa');
            if(index($relName, '@') <= 5){
              $relName =~ s/@//;
            }
          }
          $relObj{'title'} = $relName ? $relName : "";

          push @{ $relObj{'identifiers'} }, { 'type' => "zdb", 'value' => $relatedID };

          if(($endpoint eq "gvk" && $pkgType eq "NL") || $endpoint eq "natliz"){
            if(pica_value($relRecord, '004V0')){
              push @{ $relObj{'identifiers'} }, { 'type' => "doi", 'value' => pica_value($relRecord, '004V0') };
            }
          }elsif(pica_value($relRecord, '004P0')){
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
      }else{
        say STDOUT "did not find related record!";

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
        say STDOUT "no connected relation!";

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
        }else{
          $titleStats{'nonNlRelation'} = 1;
        }
        say STDOUT "Related title not in known packages: $relatedID!";

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
      say STDOUT "Trying to add relation to $relatedID";
      say STDOUT "RelType: $relationType";

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
        say STDOUT "Added relation!";
        
        if($titleStats{'usefulRelated'}){
          $titleStats{'usefulRelated'}++;
        }else{
          $titleStats{'usefulRelated'} = 1;
        }
      }elsif(any { $_ eq $relationType } @procedingTypes){
        push @{ $titleInfo{'historyEvents'} } , {
            'date' => convertToTimeStamp(($rStartYear ? $rStartYear : $end_year), ($rStartYear ? 0 : 1)),
            'to' => [\%relObj],
            'from' => [{
                'title' => $titleInfo{'name'},
                'identifiers' => $titleInfo{'identifiers'}
            }]
        };
        say STDOUT "Added relation!";
        if($titleStats{'usefulRelated'}){
          $titleStats{'usefulRelated'}++;
        }else{
          $titleStats{'usefulRelated'} = 1;
        }
      }elsif($rStartYear){
        say STDOUT "Trying to add by dates!";
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
            }else{
              $titleStats{'usefulRelated'} = 1;
            }
          }else{
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
                }else{
                  $titleStats{'usefulRelated'} = 1;
                }
              }else{ # Nachf.
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
                }else{
                  $titleStats{'usefulRelated'} = 1;
                }
              }
            }else{ # Vorg.
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
              }else{
                $titleStats{'usefulRelated'} = 1;
              }
            }
          }
          say STDOUT "Added relation!";
        }else{
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
            }else{
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
            say STDOUT "Added relation!";
          }else{
            say "Konnte Verknüpfungstyp in $id nicht identifizieren:";
            say "Titel: $start_year-".($end_year != 0 ? $end_year : "");
            say "Verknüpft: ".($rStartYear ? $rStartYear : "")."-".($rEndYear ? $rEndYear : "");
          }
        }
      }else{
        say "Konnte Verknüpfungstyp in $id nicht identifizieren:";
        say "Titel $id: $start_year-".($end_year != 0 ? $end_year : "");
        say "Verknüpft $relatedID: ".($rStartYear ? $rStartYear : "")."-".($rEndYear ? $rEndYear : "");
      }
    }
  }

  # -------------------- TIPPS (Online-Ressourcen) --------------------

  my @onlineSources;

  if(($endpoint eq "gvk" && $pkgType eq "NL") || $endpoint eq "natliz"){
    @onlineSources= @{ pica_fields($titleRecord, '009P[05]') };
  }elsif($endpoint eq "zdb" || ($endpoint eq "gvk" && $pkgType eq "AL")){
    @onlineSources= @{ pica_fields($titleRecord, '009Q') };
  }

  my $numSources = scalar @onlineSources;

  foreach my $eSource (@onlineSources){

    my ($tipp, $tippWarnings, $tippStats) = processTipp($eSource, $pkgType, $gokbType, %titleInfo);
    
    my %tipp = %{$tipp};
    my %tippWarnings = %{$tippWarnings};
    my %tippStats = %{$tippStats};
    
    if($tipp{'status'} ne "skipped"){
      push @tipps, \%tipp;
    }
    
    if($titleWarnings{'id'} ne "" ){
      push @{ $titleWarnings{'all'} }, @{ $tippWarnings{'all'} };
      push @{ $titleWarnings{'zdb'} }, @{ $tippWarnings{'zdb'} };
      push @{ $titleWarnings{'gvk'} }, @{ $tippWarnings{'gvk'} };
    }
    
    foreach my $tippStat (keys %tippStats){
      if (!$titleStats{$tippStat}){
        $titleStats{$tippStat} = $tippStats{$tippStat};
      }else{
        $titleStats{$tippStat} += $tippStats{$tippStat};
      }
    }
    
  }
 # End TIPP

  # -------------------- NO viable URL found ... --------------------

  if(scalar @tipps == 0){
    if($titleStats{'numNoUrl'}){
      $titleStats{'numNoUrl'}++;
    }else{
      $titleStats{'numNoUrl'} = 1;
    }
    say "No valid URL found, adding placeholder ($id)!";

    my $pUrl = $pkgInfo{'url'};
    my $pName = $pkgInfo{'nominalPlatform'};
    my $provider = $pkgInfo{'nominalProvider'};
    
    my $platformURL = URI->new( $pUrl );
    my $platformHost = $platformURL->authority;
    if($endpoint eq 'gvk' && $pkgType eq "NL"){
      push @{ $titleWarnings{'all'} }, {
          '009P0'=> "ZDB-URLs != GVK-URLs?"
      };

      push @{ $titleWarnings{'gvk'} }, {
          '009P0'=> "ZDB-URLs != GVK-URLs?"
      };
    }
    if($provider){
      push @tipps, {
        'medium' => "Electronic",
        'platform' => {
          'name' => $platformHost ? $platformHost : ($pName ? $pName : $provider),
          'primaryUrl' => $platformURL ? $platformURL : ""
        },
        'status' => "Current",
        'title' => {
          'identifiers' => \@{$titleInfo{'identifiers'}},
          'name' => $titleInfo{'name'},
          'type' => "Serial"
        }
      }
    }
  }

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
  }else{
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
  }else{
    say "ID ".$id." ist bereits in der Titelliste vorhanden!";

    if($titleStats{'duplicateZDBids'}){
      $titleStats{'duplicateZDBids'}++;
    }else{
      $titleStats{'duplicateZDBids'} = 1;
    }
  }
  return \@tipps, \%titleStats, \%titleWarnings;
  
} ## End TitleInstance


  ################ TIPP ################


sub processTipp {
  my %tipp;
  my ($eSource, $pkgType, $gokbType, %titleInfo) = @_;
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
  my %validComments = (
    'gvk' => ['Verlag','Digitalisierung','Agentur','Archivierung','Langzeitarchivierung','Aggregator'],
    'zdb' => ['H;','D;','A;','C;','L;','G;']
  );
  my $publicComments = "";
  my $licenceComment = "";
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
        }else{
          $tippStats{'nlURLs'} = 1;
        }
        $sourceURL =~ s/http\/\//http:\/\//;
      }
    }elsif($subField eq 'x'){
      $internalComments = $eSource[$subfPos+1];

    }elsif($subField eq 'z'){
      $publicComments = $eSource[$subfPos+1];

    }elsif($subField eq '4'){
      $licenceComment = $eSource[$subfPos+1];
    }
    $subfPos++;
  }

  if(!$sourceURL || length $sourceURL > 255 || $sourceURL eq ""){
    say "Skipping TIPP with overlong URL!";
    $tipp{'status'} = "skipped";
    return (\%tipp, \%tippWarnings, \%tippStats);
  }
  
  my $internalCommentIsValid = 0;
  
  if($endpoint eq "natliz"){
    if($pkgType eq "NL"){
      $publicComments = "NL";
    }else{
      $publicComments = $licenceComment;
    }
  }

  if($endpoint eq "gvk" && $pkgType eq "NL"){
    foreach my $vCom ( @{ $validComments{'gvk'} } ){
      if( $internalComments =~ $vCom ){
        $internalCommentIsValid = 1;
      }
    }
    $publicComments = $licenceComment;
  }elsif($endpoint eq "zdb" || ($endpoint eq "gvk" && $pkgType eq "AL")){
    foreach my $vCom ( @{ $validComments{'zdb'} } ){
      my $corCom = $vCom =~ s/;//g;

      if( $internalComments =~ $vCom || $internalComments eq $corCom ){
        $internalCommentIsValid = 1;
      }
    }
  }elsif($endpoint eq "natliz"){
    $internalCommentIsValid = 1;
  }

  if($pkgType eq "NL"){
    if($publicComments ne "Deutschlandweit zugänglich" && $publicComments ne "NL"){
      if($tippStats{'otherURLs'}){
        $tippStats{'otherURLs'}++;
      }else{
        $tippStats{'otherURLs'} = 1;
      }
      
      say STDOUT "Skipping NL-TIPP.. wrong Public Comment: $publicComments, (internal=$internalComments)";
      $tipp{'status'} = "skipped";
      return (\%tipp, \%tippWarnings, \%tippStats);
    }else{
      if($tippStats{'nlURLs'}){
        $tippStats{'nlURLs'}++;
      }else{
        $tippStats{'nlURLs'} = 1;
      }
      say STDOUT "Using NL-TIPP.. Public Comment: $publicComments, (internal=$internalComments)";
      $isNL = 1;
    }


    if($internalCommentIsValid != 1){
      say STDOUT "Skipping NL-TIPP.. wrong Internal Comment: $internalComments";
      $tipp{'status'} = "skipped";
      return (\%tipp, \%tippWarnings, \%tippStats);
    }
  }
  else {
    if($internalCommentIsValid == 1 && $publicComments ne "Deutschlandweit zugänglich" && $publicComments ne "NL"){
    
      if($tippStats{'otherURLs'}){
        $tippStats{'otherURLs'}++;
      }else{
        $tippStats{'otherURLs'} = 1;
      }
    }else{
      say STDOUT "Skipping TIPP .. wrong Type or source: $internalComments, $publicComments";
      $tipp{'status'} = "skipped";
      return (\%tipp, \%tippWarnings, \%tippStats);
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

    if(!$host){
      if($tippStats{'brokenURL'}){
        $tippStats{'brokenURL'}++;
      }else{
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
      
      say "Could not extract host of URL $url";
      $tipp{'status'} = "skipped";
      return (\%tipp, \%tippWarnings, \%tippStats);
    }else{
      if($scheme){
        $hostUrl = "$scheme://";
      }
      $hostUrl .= $host;
    }
  }else{
    say "Looks like a wrong URL!";
    
    if($tippStats{'brokenURL'}){
      $tippStats{'brokenURL'}++;
    }else{
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
    
    say "Could not extract scheme of URL $url";
    $tipp{'status'} = "skipped";
  
    return (\%tipp, \%tippWarnings, \%tippStats);
  }

  $tipp{'platform'} = {
    'name' => $host,
    'primaryUrl' => $hostUrl
  };

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
          }else{
            $endDate = convertToTimeStamp($tempYear, 1);
          }
        }

        # Volume

        if($tempVol && $tempVol ne ""){
          if($datePartPos == 0){
            $startVol = $tempVol;
          }else{
            $endVol = $tempVol;
          }
        }
        # Issue

        if($tempIss && $tempIss ne ""){
          if($datePartPos == 0){
            $startIss = $tempIss;
          }else{
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

  # -------------------- TitleInstance (in TIPP) --------------------

  $tipp{'title'} = {
    'identifiers' => \@{ $titleInfo{'identifiers'} },
    'name' => $titleInfo{'name'},
    'type' => $gokbType
  };

  return (\%tipp, \%tippWarnings, \%tippStats);
}

# Submit package/title JSON to GOKb-API

sub postData {
  my $endPointType = shift;
  my $data = shift;
  my $endPoint = $gokbCreds{'base'}."integration/".$endPointType;
  
  if($data && ref($data) eq 'HASH'){
  
    my $json_gokb = JSON->new->utf8->canonical;
    my %decData = %{ $data };  
    my $ua = LWP::UserAgent->new;

    $ua->timeout(1800);

    my $req = HTTP::Request->new(POST => $endPoint);

    $req->header('content-type' => 'application/json');
    $req->authorization_basic($gokbCreds{'username'}, $gokbCreds{'password'});
    $req->content($json_gokb->encode( \%decData ));
    
    my $resp = $ua->request($req);

    if($resp->is_success){
      if($endPointType eq 'crossReferencePackage'){
        say "Commit of package successful.";
      }

      return 0;
      
    }else{
      say "HTTP POST error code: ", $resp->code;
      say "HTTP POST error message: ", $resp->message;

      return $resp->code;
    }
  }else{
    say "Wrong endpoint or no data!";

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
    }else{
      return "";
    }
  }else{
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
  }else{
    return;
  }
}

# look up a provided publisher in ONLD.jsonld

sub searchNcsuOrgs {
  my $pubName = shift;
  my $normPubName = normalizeString($pubName);
  my $publisherMatch;

  foreach my $ncsuOrg ( @{ $orgsJSON{'@graph'} } ) {
    my %ncsuOrg = %{ $ncsuOrg };
    my $ncsuPref = $ncsuOrg{'skos:prefLabel'};
    my $ncsuPrefNorm = normalizeString($ncsuPref);

    if($normPubName eq $ncsuPrefNorm) {
      $publisherMatch = $ncsuPref;

      last;

    # Search in ncsu altLabels

    }elsif($ncsuOrg{'skos:altLabel'}){
      foreach my $altLabel ( @{ $ncsuOrg{'skos:altLabel'} } ) {
        my $altLabelNorm = normalizeString($altLabel);

        if($normPubName eq $altLabelNorm){
          $publisherMatch = $ncsuPref;

          last;
        }
      }
    }
  }
  if($publisherMatch){
    return $publisherMatch;
  }else{
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
  }else{
    return "";
  }
  if(scalar @parts == 1){
    if($end == 0){
      $date .= "-01-01";
    }elsif($end == 1){
      $date .= "-12-31";
    }
  }elsif(scalar @parts == 2){
    if($end == 0){
      $date .= "-01";
    }elsif($end == 1){
      $date .= "-31";
    }
  }elsif(scalar @parts != 3){
    return "";
  }
  $date .= " 00:00:00.000";

  return $date;
}

# Create Dates (YYYY-MM-DD) from parts as in [YYYY,MM,DD,YYYY,MM,DD]

sub transformDate {
  my $parts = shift;
  my %parts = %{ $parts };
  my @combined;

  if($parts{'sj'} ne '0'){
    my $corYear = $parts{'sj'};
    if(!looks_like_number($parts{'sj'})){
      $corYear = substr($parts{'sj'}, 0, 4);
    }
    $combined[0] = $corYear;

    if($parts{'sm'} ne '0'){
      $combined[0] .= "-".$parts{'sm'};

      if($parts{'sd'} ne 0){
        $combined[0] .= "-".$parts{'sd'};
      }
    }
  }else{
    $combined[0] = "";
  }

  if($parts{'ej'} ne '0'){
    my $corYear = $parts{'ej'};
    if(!looks_like_number($parts{'ej'})){
      $corYear = substr($parts{'ej'}, 0, 4);
    }
    $combined[1] = $corYear;

    if($parts{'em'} ne '0'){
      $combined[1] .= "-".$parts{'em'};

      if($parts{'ed'} ne '0'){
        $combined[1] .= "-".$parts{'ed'};
      }
    }
  }else{
    $combined[1] = "";
  }

  return \@combined;
}
