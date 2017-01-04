#!/usr/bin/perl

# Dieses Script dient der Synchronisation von Nationallizenz-Paketen mit der GOKb
# Es werden (je nach Aufruf) mehrere Schritte durchlaufen:
# 1. Import der Sigelinformationen aus dem Nationallizenzen-CMS (benötigt Login als Parameter)
# 2. Anreicherung der Paketinformationen mit Daten aus dem ZDB-Sigelverzeichnis
# 3. Extrahieren von Titelinformationen über die SRU-Schnittstelle des GBV
# 4. Upload der Paket- und Titeldaten in eine GOKb-Instanz
#
# Parameter:
# --packages "data_source,username,password" <- erstellt die known_seals.json
# --json (ZDB-1-...) <- neue Methode, known_seals.json muss vorhanden sein. Ohne folgendes Paketsigel werden alle Pakete bearbeitet.
# --post (URL) <- Folgt keine URL, wird die localhost Standardadresse verwendet. Nur zulässig nach --json

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
use List::Util qw(min max);
use List::MoreUtils qw(first_index);
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

# Config

## Output directories

my $packageDir = dir("packages");
my $titleDir = dir("titles");
my $warningDir = dir("warnings");
my $logDir = dir("logs");

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

## Should the TitleList be verified by the uploading Account?

my $verifyTitleList = 0;

## Restrict Titles to Journals?

my $onlyJournals = 1;

## JSON-Datei mit Paketinformationen

my $knownSeals = 'known_seals.json';

## URL der Ziel-GOKb

my $baseUrl = 'http://localhost:8080/gokb/';

## JSON-Datei mit GOKb-Organisationsdaten

my $ncsu_orgs = do {
  open(my $orgs_in, '<' , "ONLD.jsonld")
      or die("Can't open ONLD.jsonld: $!\n");
  local $/;
  <$orgs_in>
};
my %orgsJSON = %{decode_json($ncsu_orgs)} or die "Konnte JSON mit NCSU-Orgs nicht dekodieren! \n";

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
  my %logins = %{decode_json($login_data)} or die "Konnte JSON mit Logins nicht dekodieren! \n";
  if($logins{'cms'}){
    %cmsCreds = %{ $logins{'cms'} };
  }
  if($logins{'gokb'}){
    %gokbCreds = %{ $logins{'gokb'} };
  }
}

# Handle parameters

my $argP = first_index { $_ eq '--packages' } @ARGV;
my $argJ = first_index { $_ eq '--json' } @ARGV;
my $argPost = first_index { $_ eq '--post' } @ARGV;

if($ARGV[$argPost+1] && index($ARGV[$argPost+1], "http") == 0){
  $gokbCreds{'base'} = $ARGV[$argPost+1];
}
if(!$gokbCreds{'base'}){
  $gokbCreds{'base'} = "http://localhost:8080/gokb/";
}

if($argP >= 0){
  if(index($ARGV[$argP+1], "dbi") == 0){
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
        if(!$gokbCreds{'username'} || !$gokbCreds{'password'}){
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
        createJSON($post, $ARGV[$argJ+1]);
      }else{
        say "Pakete abgerufen, erstelle JSONs!";
        createJSON($post);
      }
    }else{
      die "Erstelle keine JSONs, Sigeldatei wurde nicht erstellt!";
    }
  }else{
      say "Erstelle nur Paketdatei!";
      getSeals($cmsCreds{'base'},$cmsCreds{'username'},$cmsCreds{'password'});
  }
}elsif($argJ >= 0){
  if(-e $knownSeals){
    my $post = 0;
    if($argPost >= 0){
      if(!$gokbCreds{'username'} || !$gokbCreds{'password'}){
        say STDOUT "GOKb-Benutzername:";
        $gokbCreds{'username'} = <STDIN>;
        say STDOUT "GOKb-Passwort:";
        ReadMode 2;
        $gokbCreds{'password'} = <STDIN>;
        ReadMode 0;
        chomp $gokbCreds{'password'};
      }
      if($gokbCreds{'username'} && $gokbCreds{'password'}){
        $post = 1;
      }else{
        say "Kein Benutzername/Passwort, überspringe GOKb-Import!";
      }
    }
    if($ARGV[$argJ+1] && index($ARGV[$argJ+1], "ZDB") == 0){
      my $filterSigel = $ARGV[$argJ+1];
      say "Paketdatei gefunden, erstelle JSON für $filterSigel!";
      createJSON($post, $filterSigel);
    }else{
      say "Paketdatei gefunden, erstelle JSONs!";
      createJSON($post);
    }
  }else{
    say "Paketdatei nicht vorhanden! Zum Erstellen mit Parameter '--packages \"data_source,username,password\"' starten!";
  }
}

# No parameters

if(scalar @ARGV == 0){

  say STDOUT "Keine Parameter gefunden!";
  say STDOUT "Mögliche Parameter sind:";
  say STDOUT "'--packages \"data_source,username,password\"'";
  say STDOUT "'--json [\"Sigel\"]'";
  say STDOUT "'--post [\"URL\"]'";
}

# Query Sigelverzeichnis via SRU for package metadata

sub getZdbName {
  my $sig = shift;
  print strftime '%Y-%m-%d %H:%M:%S', localtime;
  print " - Sigel: $sig \n";
  my $name = "";
  my $provider = "";
  my $platform = "";
  my $authority = "";
  my %attrs = (
      base => 'http://services.dnb.de/sru/bib',
      query => 'woe='.$sig,
      recordSchema => 'PicaPlus-xml',
      parser => 'ppxml'
  );
  my $importer = Catmandu::Importer::SRU->new(%attrs) or die " - Abfrage über ".$attrs{'base'}." fehlgeschlagen!\n";

  $importer->each(
    sub {
      my $packageInstance = shift;

      if(pica_value($packageInstance, '035Ea') ne 'I' && pica_value($packageInstance, '008Hd') eq $sig){
        my $messyName = pica_value($packageInstance, '029Aa');
        my $bracketPos = index($messyName, '[');
        $name = substr($messyName, 0, $bracketPos-1);
        $name =~ s/^\s+|\s+$//g;

        $provider = pica_value($packageInstance, '035Pg') ? pica_value($packageInstance, '035Pg') : "";
        if(index($provider, ";") >= 0){
          ($provider, $platform) = split(";",$provider);
          $provider =~ s/^\s+|\s+$//g;
          $platform =~ s/^\s+|\s+$//g;
        }
        if(index($provider, "(") >= 0){
          $provider = substr($provider, 0, index($provider, "(")-1);
        }
        $authority = pica_value($packageInstance, '032Pa') ? pica_value($packageInstance, '032Pa') : "";
      }else{
        print strftime '%Y-%m-%d %H:%M:%S', localtime;
        print " - Überspringe Eintrag für ".$sig.": 035Ea: ".pica_value($packageInstance, '035Ea')." - 008Hd: ".pica_value($packageInstance, '008Hd')." \n";
      }
    }
  );
  return ($name, $provider, $platform, $authority);
}

# Import ZDB seals via SQL and write them to JSON

sub getSeals {
  my ($host, $username, $password) = @_;
  my $dbh = DBI->connect($host,$username,$password,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

  # Request package id, seal

  my $stmt = qq(select zuid, seal as sigel from lmodels where meta_type = 'NLLicenceModelStandard' and wf_state='published';);

  # Request linked institutions

  my $orgStmt = qq(SELECT zobjects.licences.zuid,
    zobjects.licences.wf_state,
    zobjects.nlinstitutions.title as institution,
    zobjects.nlinstitutions.sigel as sigel
    FROM zobjects.licences, zobjects.nlinstitutions
    WHERE zobjects.licences.lmodel = ?
    AND zobjects.licences.lowner::uuid=zobjects.nlinstitutions.zuid;
  );
  my $stO = $dbh->prepare( $orgStmt );
  my $sth = $dbh->prepare( $stmt );
  my $rv = $sth->execute() or die $DBI::errstr;
  my $JSON = JSON->new->utf8->canonical;
  my %alljson;
  if(-e $knownSeals){
    copy($knownSeals, $knownSeals."_last.json");
  }
  open( my $out, '>', $knownSeals ) or die "Failed to open $knownSeals for writing";

  if($rv < 0){
    print $DBI::errstr;
  }

  # Process packages

  while(my @row = $sth->fetchrow_array()) {
    my ($zuid, $pkgSigel) = @row;
    if($pkgSigel ne 'ZDB-1-TEST'){
      my ($name,$provider, $platform, $authority) = getZdbName($pkgSigel);
      my %pkg = (
        sigel => $pkgSigel,
        zuid => $zuid,
        name => $name,
        authority => $authority,
        provider => $provider,
        platform => $platform,
        cmsOrgs => [],
        zdbOrgs => [],
        orgStats => {
          'numValidSig' => 0,
          'numOhneSig' => 0,
          'numCorrectSig' => 0,
          'numWrongSig' => 0
        }
      );

      ## Process linked Orgs

      my $orgs = $stO->execute($zuid) or die $DBI::errstr;
      while(my @orgRow = $stO->fetchrow_array()){
        my $orgStatus = $orgRow[1];
        my $orgName = $orgRow[2];
        my $orgSigel = $orgRow[3] ? $orgRow[3] : undef;
        my $fixedSigel = undef;

        ### Verify Seals

        unless(!$orgSigel || $orgSigel =~ /^DE\-[a-zA-Z0-9üäöÜÄÖ]*$/){
          if($orgSigel =~ /^\s?[a-zA-Z0-9üäöÜÄÖ]+\s?[a-zA-Z0-9üäöÜÄÖ]*\s?$/){
            $fixedSigel = $orgSigel;
            $fixedSigel =~ s/\s//g;
            $fixedSigel = "DE-".$fixedSigel;

            # Too many Timeouts, disabling SRU verification for now...

#             my %bibAttrs = (
#                 base => 'http://services.dnb.de/sru/bib',
#                 query => 'woe='.$fixedSigel,
#                 recordSchema => 'PicaPlus-xml',
#                 parser => 'ppxml'
#             );
#             my $orgImporter = Catmandu::Importer::SRU->new(%bibAttrs) or die " - Abfrage über ".$bibAttrs{'base'}." fehlgeschlagen!\n";
#             my $sruOrg = $orgImporter->first();
#             if($sruOrg){
#               $pkg{'orgStats'}{'numValidSig'}++;
#             }else{
#               $fixedSigel = undef;
#             }
            $pkg{'orgStats'}{'numValidSig'}++;
          }else{
            $pkg{'orgStats'}{'numWrongSig'}++;
          }
        }
        if(!$orgSigel){
          $pkg{'orgStats'}{'numOhneSig'}++;
        }elsif($orgSigel =~ /^DE\-[a-zA-Z0-9üäöÜÄÖ]*$/){
          $fixedSigel = $orgSigel;
          $pkg{'orgStats'}{'numCorrectSig'}++;
        }
        push @{ $pkg{'cmsOrgs'} }, {'name' => $orgName, 'sigel' => $orgSigel, 'fixedSigel' => $fixedSigel, 'status' => $orgStatus};
      }
      $pkg{'orgStats'}{'numCms'} = scalar @{ $pkg{'cmsOrgs'} };

      my %zdbAttrs = (
          base => 'http://services.dnb.de/sru/zdb',
          query => 'isil='.$pkgSigel,
          recordSchema => 'PicaPlus-xml',
          _max_results => 1,
          parser => 'ppxml'
      );
      my $titleImporter = Catmandu::Importer::SRU->new(%zdbAttrs) or die " - Abfrage über ".$zdbAttrs{'base'}." fehlgeschlagen!\n";
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
  };
  $dbh->disconnect;

  say $out $JSON->pretty(1)->encode( \%alljson );

  close($out);
  sleep 1;
  return 0;
}

# Create packages, tipps and titles as GOKb-JSON (and trigger upload if requested)

sub createJSON {

  my $postData = $_[0];
  my ($filter) = $_[1];

  my $json_seals = do {
    open(my $json_fh, '<' , $knownSeals)
        or die("Can't open \$knownSeals\": $!\n");
    local $/;
    <$json_fh>
  };

  # Input JSON handling

  my %known = %{decode_json($json_seals)} or die "JSON nicht vorhanden!\n";
  my %knownSelection;
  if($filter){
    if($known{$filter}){
      $knownSelection{$filter} = $known{$filter};
      say "Generating JSON only for $filter!";
    }else{
      say "Paket nicht bekannt!";
      return -1;
    }
  }else{
    %knownSelection = %known;
    say "Generating JSON for all packages!";
  }

  $packageDir->mkpath( { verbose => 0 } );
  $titleDir->mkpath( { verbose => 0 } );
  $warningDir->mkpath( { verbose => 0 } );

  # Warnings

  my $out_warnings;
  my $out_warnings_zdb;
  my $out_warnings_gvk;

  if(!$filter){
    my $wfile = $warningDir->file("Warnings_all.json");
    $wfile->touch();
    $out_warnings = $wfile->openw();

    my $wzfile = $warningDir->file("Warnings_zdb_all.json");
    $wzfile->touch();
    $out_warnings_zdb = $wzfile->openw();

    my $wgfile = $warningDir->file("Warnings_gvk_all.json");
    $wgfile->touch();
    $out_warnings_gvk = $wgfile->openw();
  }
  else{
    my $wdir = $warningDir->subdir($filter);
    $wdir->mkpath({verbose => 0});

    my $wfile = $wdir->file("Warnings_$filter.json");
    $wfile->touch();
    $out_warnings = $wfile->openw();

    my $wzfile = $wdir->file("Warnings_zdb_$filter.json");
    $wzfile->touch();
    $out_warnings_zdb = $wzfile->openw();

    my $wgfile = $wdir->file("Warnings_gvk_$filter.json");
    $wgfile->touch();
    $out_warnings_gvk = $wgfile->openw();
  }

  # Statistics

  my $titlesTotal = 0;
  my $packagesTotal = 0;
  my $duplicateISSNs = 0;
  my $wrongISSN = 0;
  my $skippedPackages = "";
  my $pubFromAuthor = 0;
  my $pubFromCorp = 0;
  my $numNoUrl = 0;
  my $noPubMatch = 0;
  my $noPubGiven = 0;

  # Collections

  my @allTitles;
  my %globalIDs;
  my %authorityNotes;
  my %authorityNotesZDB;
  my %authorityNotesGVK;

  # Output file handling

  my $json_warning = JSON->new->utf8->canonical;
  my $json_warning_zdb = JSON->new->utf8->canonical;
  my $json_warning_gvk = JSON->new->utf8->canonical;
  my $json_titles = JSON->new->utf8->canonical;

  # Start timer

  my $startTime = time();



  ################ PACKAGE ################



  foreach my $sigel (keys %knownSelection){

    my $currentTitle = 0;
    my $noPublisher = 0;
    my %allISSN;
    my %allIDs;
    my %package;

    my $json_pkg = JSON->new->utf8->canonical;
    my $out_pkg;

    if($filter){
      if(-e "$sigel.json"){
        copy("$sigel.json", $sigel."_last.json");
      }
      my $pfile = $packageDir->file("$sigel.json");
      $pfile->touch();
      $out_pkg = $pfile->openw();
    }

    say "Processing Package ".($packagesTotal + 1).", ".$sigel."...";
    if($onlyJournals == 1 && scalar @{ $knownSelection{$sigel}{'zdbOrgs'} } == 0){
      say "Paket hat keine verknüpften Institutionen in der ZBD. Überspringe Paket.";
      next;
    }

    ## Package Header

    my $userListVer = "";
    my $listVerDate = "";

    if($verifyTitleList != 0 && $gokbCreds{'username'}){
      $userListVer = $gokbCreds{'username'};
      $listVerDate = convertToTimeStamp(strftime('%Y-%m-%d', localtime));
    }

    my $provider = $knownSelection{$sigel}{'provider'};
    my $pkgName = $knownSelection{$sigel}{'name'};
    $pkgName =~ s/:\s//g;
    my $pkgYear = strftime '%Y', localtime;
    my $pkgPlatform = $knownSelection{$sigel}{'platformURL'} ? $knownSelection{$sigel}{'platformURL'} : "";

    $package{'packageHeader'} = {
      name => "$provider: $pkgName: NL $pkgYear",
      # identifiers => { name => "ISIL", value => $sigel },
      additionalProperties => [],
      variantNames => [{
        variantName => $sigel
      }],
      scope => "",
      listStatus => "Checked",
      breakable => "No",
      consistent => "Yes",
      fixed => "Yes",
      paymentType => "",
      global => "Consortium",
      listVerifier => "",
      userListVerifier => $userListVer,
      nominalPlatform => $pkgPlatform,
      nominalProvider => $provider,
      listVerifiedDate => $listVerDate,
      source => {
          url => "http://sru.gbv.de/gvk",
          name => "GVK-SRU",
          normname => "GVK_SRU"
      },
      curatoryGroups => [{
        curatoryGroup => "LAS:eR"
      }],
    };

    $package{'tipps'} = [];

    my $qryString = 'pica.xpr='.$sigel;
    if($onlyJournals == 1){
      $qryString .= ' and (pica.mak=Obvz or pica.mak=Obv)';
    }
    $qryString .= ' sortBy year/sort.ascending';
    my %attrs = (
      base => 'http://sru.gbv.de/gvk',
      query => $qryString,
      recordSchema => 'picaxml',
      parser => 'picaxml',
      _max_results => 5
    );

    my $sruTitles = Catmandu::Importer::SRU->new(%attrs) or die "Abfrage über ".$attrs{'base'}." fehlgeschlagen!\n";



    ################ TITLEINSTANCE ################



    while (my $titleRecord = $sruTitles->next){
      $currentTitle++;
      my $materialType = pica_value($titleRecord, '002@0');
      my $gokbType;
      my $gokbMedium;
      my $ppn = pica_value($titleRecord, '003@0');
      my %titleInfo;
      my $id;
      my $eissn;
      my @relatedPrev = ();

      my @titleWarnings;
      my @titleWarningsZDB;
      my @titleWarningsGVK;

      # Process material code

      if(substr($materialType, 1, 1) eq 'b'){
        $gokbType = "Serial";
      }else{
        $gokbType = "";
      }

      if(substr($materialType, 1, 1) eq 'b'){
        $gokbMedium = "Journal";
      }else{
        $gokbMedium = "";
      }

      # -------------------- Identifiers --------------------

      $titleInfo{'identifiers'} = [];

      ## PPN

      push @{ $titleInfo{'identifiers'} } , {
        'type' => "gvk_ppn",
        'value' => $ppn
      };

      ## Journal-IDs

      if(substr($materialType, 0, 2) eq 'Ob'){

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
          print "Titel mit ppn ".pica_value($titleRecord, '003@0')." hat keine ZDB-ID! Überspringe Titel..\n";
          push @titleWarnings, { '006Z0' => pica_values($titleRecord, '006Z0') };
          push @titleWarningsGVK, { '006Z0' => pica_values($titleRecord, '006Z0') };
          next;
        }

        ## eISSN

        if(pica_value($titleRecord, '005A0')){
          my $eissn = formatISSN(pica_value($titleRecord, '005A0'));

          if($eissn eq ""){
            say "ISSN ".pica_value($titleRecord, '005A0')." in Titel $id scheint ungültig zu sein!";
            push @titleWarnings, {
              '005A0' => $eissn
            };
            push @titleWarningsZDB, {
              '005A0' => $eissn
            };
          }elsif($allISSN{$eissn} || ( $globalIDs{$id} && $globalIDs{$id}{'eissn'} ne $eissn ) ){
            say "eISSN $eissn in Titel $id wurde bereits vergeben!";
            $duplicateISSNs++;
            push @titleWarnings, {
              '005A0' => $eissn,
              'comment' => 'gleiche eISSN nach Titeländerung?'
            };
            push @titleWarningsZDB, {
              '005A0' => $eissn,
              'comment' => 'gleiche eISSN nach Titeländerung?'
            };
          }else{
            push @{ $titleInfo{'identifiers'}} , {
              'type' => "eissn",
              'value' => $eissn
            };
            $allISSN{$eissn} = pica_value($titleRecord, '021Aa');
          }
        }

        ## pISSN

        if(pica_value($titleRecord, '005P0')){
          my $pissn = formatISSN(pica_value($titleRecord, '005P0'));
          if($pissn eq ""){
            say "Parallel-ISSN ".pica_value($titleRecord, '005P0')." in Titel $id scheint ungültig zu sein!";
            push @titleWarnings, {
              '005A0' => $pissn
            };
            push @titleWarningsZDB, {
              '005A0' => $pissn
            };
          }elsif($allISSN{$pissn}){
            say "Parallel-ISSN $pissn in Titel $id wurde bereits als eISSN vergeben!";
            $wrongISSN++;
            push @titleWarnings, {
              '005P0' => $pissn,
              'comment' => 'gleiche Vorgänger-eISSN als Parallel-ISSN?'
            };
            push @titleWarningsZDB, {
              '005P0' => $pissn,
              'comment' => 'gleiche Vorgänger-eISSN als Parallel-ISSN?'
            };
          }
        }
      }

      ## Andere Identifier, z.B. OCLC-No.

#       my @otherIdentifiers = @{ pica_fields($titleRecord, '006X') };
#
#       if(scalar @otherIdentifiers > 0){
#         foreach my $otherID (@otherIdentifiers){
#           my @otherID = @{ $otherID };
#           my $subfPos = 0;
#           foreach my $subField (@otherID){
#             if($subField eq 'c'){
#               push @{ $titleInfo{'identifiers'}} , { 'type' => $otherID[$subfPos+1], 'value' => $otherID[$subfPos+2] eq '0' ? $otherID[$subfPos+3] : "" };
#               # print "other ID: ".$otherID[$subfPos+1]." = ".$otherID[$subfPos+3]."\n";
#             }
#             $subfPos++;
#           }
#         }
#       }

      # Check, if the title is a journal
      # (shouldn't be necessary since it should be included in the search query)

      if($onlyJournals == 1 && substr($materialType, 0, 2) ne 'Ob'){
        say "Überspringe Titel ".pica_value($titleRecord, '021Aa').", Materialcode: ".$materialType;
        next;
      }
      if(pica_value($titleRecord, '006Z0')){
        say STDOUT "Verarbeite Titel ".$currentTitle." von Paket ".$sigel."(".$id.")";
      }else{
        say STDOUT "Verarbeite Titel ".$currentTitle." von Paket ".$sigel."(".$ppn.")";
      }

      # -------------------- Title --------------------

      if(pica_value($titleRecord, '025@a')){
        my $titleField = pica_value($titleRecord, '025@a');
        if(index($titleField, '@') <= 5){
          $titleField =~ s/@//;
        }
        $titleInfo{'name'} = $titleField;
        
      }elsif(pica_value($titleRecord, '021Aa')){
        my $titleField = pica_value($titleRecord, '021Aa');
        if(index($titleField, '@') <= 5){
          $titleField =~ s/@//;
        }
        $titleInfo{'name'} = $titleField;
        
      }else{
        say "Keinen Titel für ".$ppn." erkannt, überspringe Titel!";
        push @titleWarnings, { '021Aa' => pica_value($titleRecord, '021Aa') };
        push @titleWarningsZDB, { '021Aa' => pica_value($titleRecord, '021Aa') };
        next;
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
        foreach my $subField (@releaseNote){
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
        $start_year = $releaseStart{'year'}
      }

      if(pica_value($titleRecord, '011@b')){
        if($start_year != 0 && pica_value($titleRecord, '011@b') >= $start_year){
          $end_year = pica_value($titleRecord, '011@b');
        }
      }elsif($releaseEnd{'year'} ne ""){
        if($start_year != 0 && $releaseEnd{'year'} >= $start_year ){
          $end_year = $releaseEnd{'year'}
        }
      }

      if(pica_value($titleRecord, '011@a') && $releaseStart{'year'} eq pica_value($titleRecord, '011@a')){
        if(looks_like_number($releaseStart{'month'})){
          $start_month = $releaseStart{'month'};
        }elsif($releaseStart{'month'} ne ""){
          push @titleWarnings, { '031Nc' => pica_value($titleRecord, '031Nc') };
          push @titleWarningsZDB, { '031Nc' => pica_value($titleRecord, '031Nc') };
        }
        if(looks_like_number($releaseStart{'day'}) && $start_month != 0){
          $start_day = $releaseStart{'day'};
        }
      }
      if(pica_value($titleRecord, '011@b') && $releaseEnd{'year'} eq pica_value($titleRecord, '011@b')){
        if(looks_like_number($releaseEnd{'month'})){
          $end_month = $releaseEnd{'month'};
        }elsif($releaseEnd{'month'} ne ""){
          push @titleWarnings, { '031Nm' => pica_value($titleRecord, '031Nm') };
          push @titleWarningsZDB, { '031Nm' => pica_value($titleRecord, '031Nm') };
        }
        if(looks_like_number($releaseEnd{'day'}) && $end_month != 0){
          $end_day = $releaseEnd{'day'};
        }
      }

      my @dates = (
        $start_year,
        $start_month,
        $start_day,
        $end_year,
        $end_month,
        $end_day
      );

      my @dts = transformDate(\@dates);

      $titleInfo{'publishedFrom'} = convertToTimeStamp($dts[0][0], 0);

      $titleInfo{'publishedTo'} = convertToTimeStamp($dts[0][1], 1);

      # -------------------- Publisher --------------------

      $titleInfo{'publisher_history'} = [];
      
      my @possiblePubs = @{ pica_fields($titleRecord, '033A') };
      my $checkPubs = pica_value($titleRecord, '033An');
      my @altPubs = @{ pica_fields($titleRecord, '033B[05]') };
      my $authorField = pica_value($titleRecord, '021Ah');
      my $corpField = pica_value($titleRecord, '029Aa');

      if(!$checkPubs){
        $noPubGiven++;
      }
      
      if(scalar @possiblePubs > 0){
        foreach my $pub (@possiblePubs) {
          my @pub = @{ $pub };
          my $tempPub;
          my $pubStart;
          my $pubEnd;
          my $subfPos = 0;
          my $preCorrectedPub = "";
          foreach my $subField (@pub){

            if($subField eq 'n'){
              if($tempPub){
                push @titleWarnings, { '033A' => \@pub };
                push @titleWarningsZDB, { '033A' => \@pub };
                push @titleWarningsGVK, { '033A' => \@pub };
              }
              $preCorrectedPub = $pub[$subfPos+1];
              $tempPub = $pub[$subfPos+1];
            }
            if($subField eq 'h'){

              if( $pub[$subfPos+1] =~ /[a-zA-Z\.,\(\)]+/ ) {
                push @titleWarnings, { '033Ah' => $pub[$subfPos+1] };
                push @titleWarningsZDB, { '033Ah' => $pub[$subfPos+1] };
              }
              my ($tempStart) = $pub[$subfPos+1] =~ /([0-9]{4})\/?[0-9]{0,2}\s?-/;

              if($tempStart && looks_like_number($tempStart)) {
                $pubStart = convertToTimeStamp($tempStart, 0);
              }

              my ($tempEnd) = $pub[$subfPos+1] =~ /-\s?([0-9]{4})/;

              if($tempEnd && looks_like_number($tempEnd)) {
                $pubEnd = convertToTimeStamp($tempEnd, 1);
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

          if(index(lc($tempPub), "publ") >= 0){

            $tempPub =~ s/(^|\s)([pP]ubl)\.?(\s|$)/$1Pub$3/g;
          }

          if(index(lc($tempPub), "assoc") >= 0){

            $tempPub =~ s/(^|\s)([Aa]ssoc)\.?(\s|$)/$1Association$3/g;
          }

          if(index(lc($tempPub), "soc") >= 0){

            $tempPub =~ s/(^|\s)([Ss]oc)\.?(\s|$)/$1Society$3/g;
          }

          if(index(lc($tempPub), "univ") >= 0){

            $tempPub =~ s/(^|\s)([Uu]niv)\.?(\s|$)/$1University$3/g;
          }

          if(index(lc($tempPub), "acad") >= 0){

            $tempPub =~ s/(^|\s)([Aa]cad)\.?(\s|$)/$1Academic$3/g;
          }

          ## Verlag verifizieren & hinzufügen

          my $ncsuPub = searchNcsuOrgs($tempPub);

          if($ncsuPub ne '0'){
            push @{ $titleInfo{'publisher_history'}} , {
                'name' => $ncsuPub,
                'startDate' => $pubStart ? $pubStart : "",
                'endDate' => $pubEnd ? $pubEnd : "",
                'status' => "Active"
            };
          }elsif($ncsuPub eq '0' || $tempPub =~ /[\[\]]/ || $tempPub =~ /u\.\s?a\./){
            $noPubMatch++;
            push @titleWarnings, { '033An' => $preCorrectedPub };
            push @titleWarningsZDB, { '033An' => $preCorrectedPub };
          }
        }
      }

      ## Im Autor- bzw. Körperschaftsfeld nach Ersatz suchen

      if(scalar @{ $titleInfo{'publisher_history'} } == 0) {

        if($authorField){
          my $ncsuAuthor = searchNcsuOrgs($authorField);
          if($ncsuAuthor ne '0'){
            push @{ $titleInfo{'publisher_history'}} , {
                'name' => $ncsuAuthor,
                'startDate' => convertToTimeStamp($dts[0][0], 0),
                'endDate' => convertToTimeStamp($dts[0][1], 1),
                'status' => ""
            };
            $pubFromAuthor++;
          }
          # print "Used author $authorField as publisher.\n";
        }elsif($corpField){
          my $ncsuCorp = searchNcsuOrgs($corpField);
          if($ncsuCorp ne '0'){
            push @{ $titleInfo{'publisher_history'}} , {
                'name' => $ncsuCorp,
                'startDate' => convertToTimeStamp($dts[0][0], 0),
                'endDate' => convertToTimeStamp($dts[0][1], 1),
                'status' => ""
            };
            $pubFromCorp++;
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
        my $relatedDates;
        my $relatedStartYear;
        my $relatedEndYear;
        my $subfPos = 0;
        foreach my $subField (@relTitle){
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

            my ($tempStartYear) = $relTitle[$subfPos+1] =~ /([0-9]{4})\s?-/;
            my ($tempEndYear) = $relTitle[$subfPos+1] =~ /-\s?([0-9]{4})[^\.]?/;

            if($tempEndYear){
              $relatedEndYear = $tempEndYear;
              if($subField eq 'd') {
                push @titleWarnings, {
                  '039Ed' => $relTitle[$subfPos+1],
                  'comment' => 'Datumsangaben gehören in Unterfeld f.'
                };
                push @titleWarningsGVK, {
                  '039Ed' => $relTitle[$subfPos+1],
                  'comment' => 'Datumsangaben gehören in Unterfeld f.'
                };
              }
            }
            if($tempStartYear){
              $relatedStartYear = $tempStartYear;
              if($subField eq 'd') {
                push @titleWarnings, {
                  '039Ed' => $relTitle[$subfPos+1],
                  'comment' => 'Datumsangaben gehören in Unterfeld f.'
                };
                push @titleWarningsGVK, {
                  '039Ed' => $relTitle[$subfPos+1],
                  'comment' => 'Datumsangaben gehören in Unterfeld f.'
                };
              }
            }
            if($subField eq 'f'){
              $relatedDates = $relTitle[$subfPos+1];
              unless($relatedDates =~ /[0-9]{4}\s?-\s?[0-9]{0,4}/){
                push @titleWarnings, { '039Ef' => $relTitle[$subfPos+1] };
                push @titleWarningsGVK, { '039Ef' => $relTitle[$subfPos+1] };
              }
            }
          }
          $subfPos++;
        }
        if($relatedID){
          my $isInList = $allIDs{$relatedID} ? "yes" : "no";

          if($relationType && $relationType ne ( 'Druckausg' || 'Druckausg.' )){
            push @relatedPrev, $relatedID;
          }
          if($allIDs{$relatedID} && ref($allIDs{$relatedID}{'connected'}) eq 'ARRAY'){
            @connectedIDs = @{ $allIDs{$relatedID}{'connected'} };
          }
        }
        if($relatedID && $relationType && $relationType ne 'Druckausg' && $relationType ne 'Druckausg.' && $allIDs{$relatedID} && $relatedStartYear && scalar @connectedIDs > 0 && $id ~~ @connectedIDs){

          if($relatedEndYear){
            if($relatedEndYear < $start_year){ # Vorg.
              push @{ $titleInfo{'historyEvents'} } , {
                  'date' => convertToTimeStamp($start_year, 0),
                  'from' => [{
                      'title' => $relName ? $relName : "",
                      'identifiers' => [{
                          'type' => "zdb",
                          'value' => $relatedID
                      }]
                  }],
                  'to' => [{
                      'title' => $titleInfo{'name'},
                      'identifiers' => $titleInfo{'identifiers'}
                  }]
              };
            }else{
              if($end_year != 0){
                if($relatedEndYear <= $end_year){ # Vorg.
                  push @{ $titleInfo{'historyEvents'} } , {
                      'date' => convertToTimeStamp($relatedEndYear, 1),
                      'from' => [{
                          'title' => $relName ? $relName : "",
                          'identifiers' => [{
                              'type' => "zdb",
                              'value' => $relatedID
                          }]
                      }],
                      'to' => [{
                          'title' => $titleInfo{'name'},
                          'identifiers' => $titleInfo{'identifiers'}
                      }]
                  };
                }else{ # Nachf.
                  push @{ $titleInfo{'historyEvents'} } , {
                      'date' => convertToTimeStamp($end_year, 1),
                      'to' => [{
                          'title' => $relName ? $relName : "",
                          'identifiers' => [{
                              'type' => "zdb",
                              'value' => $relatedID
                          }]
                      }],
                      'from' => [{
                          'title' => $titleInfo{'name'},
                          'identifiers' => $titleInfo{'identifiers'}
                      }]
                  };
                }
              }else{ # Vorg.
                push @{ $titleInfo{'historyEvents'} } , {
                    'date' => convertToTimeStamp($relatedEndYear, 1),
                    'from' => [{
                        'title' => $relName ? $relName : "",
                        'identifiers' => [{
                            'type' => "zdb",
                            'value' => $relatedID
                        }]
                    }],
                    'to' => [{
                        'title' => $titleInfo{'name'},
                        'identifiers' => $titleInfo{'identifiers'}
                    }]
                };
              }
            }
          }else{
            if($end_year != 0){ # Nachf.
              push @{ $titleInfo{'historyEvents'} } , {
                  'date' => convertToTimeStamp($end_year, 1),
                  'to' => [{
                      'title' => $relName ? $relName : "",
                      'identifiers' => [{
                          'type' => "zdb",
                          'value' => $relatedID
                      }]
                  }],
                  'from' => [{
                      'title' => $titleInfo{'name'},
                      'identifiers' => $titleInfo{'identifiers'}
                  }]
              };
              if($relatedStartYear < $start_year){ # Vorg.
                push @{ $titleInfo{'historyEvents'} } , {
                    'date' => convertToTimeStamp($start_year, 0),
                    'from' => [{
                        'title' => $relName ? $relName : "",
                        'identifiers' => [{
                            'type' => "zdb",
                            'value' => $relatedID
                        }]
                    }],
                    'to' => [{
                        'title' => $titleInfo{'name'},
                        'identifiers' => $titleInfo{'identifiers'}
                    }]
                };
              }
            }else{
              say "Konnte keinen direkten Vorgänger bzw. Nachfolger in $id ausmachen: ";
              say "$start_year-".($end_year != 0 ? $end_year : "")." und $relatedStartYear-".($relatedEndYear ? $relatedEndYear : "");
            }
          }
        }
      }

      # -------------------- TIPPS (Online-Ressourcen) --------------------

      my @onlineSources = @{ pica_fields($titleRecord, '009P[05]') };
      my $noViableUrl = 1;



      ################ TIPP ################



      foreach my $eSource (@onlineSources){
        my %tipp;
        my @eSource = @{ $eSource };
        my $sourceURL = "";
        my $internalComments = "";
        my $publicComments = "";
        my $subfPos = 0;

        foreach my $subField (@eSource){
          if($subField eq 'a'){
            $sourceURL = $eSource[$subfPos+1];
            if($sourceURL =~ /http\/\//){
              push @titleWarnings , {'009P0'=> $sourceURL};
              push @titleWarningsGVK , {'009P0'=> $sourceURL};
              $sourceURL =~ s/http\/\//http:\/\//;
            }
            if(index($sourceURL, '=u ') == 0){
              $sourceURL =~ s/=u\s//;
            }
          }elsif($subField eq 'x'){
            $internalComments = $eSource[$subfPos+1];
          }elsif($subField eq 'z'){
            $publicComments = $eSource[$subfPos+1];
          }
          $subfPos++;
        }

        if(!$sourceURL || length $sourceURL > 255 || $sourceURL eq ""){
          say "Skipping TIPP in $id with overlong URL!";
          next;
        }
        if($publicComments ne "Deutschlandweit zugänglich"){
          next;
        }else{
          $noViableUrl = 0;
        }

        $tipp{'status'} = "Current";
        $tipp{'medium'} = "Electronic";
        $tipp{'accessStart'} = "";
        $tipp{'accessEnd'} = "";
        $tipp{'url'} = $sourceURL;

        # -------------------- Platform --------------------

        my $url = URI->new( $sourceURL );
        my $host;
        if($url->has_recognized_scheme){
          $host = $url->authority;
          if(!$host){
            push @titleWarnings , {'009P0'=> $sourceURL};
            push @titleWarningsZDB , {'009P0'=> $sourceURL};
            next;
          }
        }else{
          say "Looks like a wrong URL >".$id;
          push @titleWarnings , {'009P0'=> $sourceURL};
          push @titleWarningsZDB , {'009P0'=> $sourceURL};
          next;
        }

        $tipp{'platform'} = {
          'name' => $host,
          'primaryUrl' => $host
        };

        # -------------------- Coverage --------------------

        my $startVol = "";
        my $startIss = "";
        my $startDate = "";
        my $endVol = "";
        my $endIss = "";
        my $endDate = "";
        if(index($internalComments, 'Verlag') >= 0 || index($internalComments, 'Digitalisierung') >= 0){
          my @fieldParts = split(';', $internalComments);
          if(scalar @fieldParts == 2){

            # Split into start and end

            my @datesParts = split /\-/, $fieldParts[1];
            my $datePartPos = 0;

            foreach my $dp (@datesParts){
              if($dp =~ /[a-zA-Z]+/){
                push @titleWarnings , {'009P[05]'=> $fieldParts[1]};
                push @titleWarningsZDB , {'009P[05]'=> $fieldParts[1]};
              }
              my ($tempVol) = $dp =~ /([0-9]+)\.[0-9]{4}/;
              my ($tempYear) = $dp =~ /\.?([0-9]{4})/;
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
              }else{
                if($datePartPos == 0){
                  if($startVol eq "" && $tempYear && $tempYear <= $start_year){
                    if(pica_value($titleRecord, '031Nd')){
                      $startVol = pica_value($titleRecord, '031Nd');
                    }
                  }
                }else{
                  if($endVol eq "" && $tempYear && $tempYear >= $end_year){
                    if(pica_value($titleRecord, '031Nn')){
                      $endVol = pica_value($titleRecord, '031Nn');
                    }
                  }
                }
              }

              # Issue

              if($tempIss && $tempIss ne ""){
                if($datePartPos == 0){
                  $startIss = $tempIss;
                }else{
                  $endIss = $tempIss;
                }
              }else{
                if($datePartPos == 0){
                  if($startIss eq "" && $tempYear && $tempYear <= $start_year){
                    if(pica_value($titleRecord, '031Ne')){
                      $startIss = pica_value($titleRecord, '031Ne');
                    }
                  }
                }else{
                  if($endIss eq "" && $tempYear && $tempYear >= $end_year){
                    if(pica_value($titleRecord, '031No')){
                      $endIss = pica_value($titleRecord, '031No');
                    }
                  }
                }
              }
              $datePartPos++;
            }
          }
        }

        $tipp{'coverage'} = [];
        push @{ $tipp{'coverage'} } , {
          'startDate' => $startDate,
          'startVolume' => $startVol,
          'startIssue' => $startIss,
          'endDate' => $endDate,
          'endVolume' => $endVol,
          'endIssue' => $endIss,
          'coverageDepth' => "Fulltext",
          'coverageNote' => "NL-DE",
          'embargo' => ""
        };

        # -------------------- TitleInstance (in TIPP) --------------------

        $tipp{'title'} = {
          'identifiers' => [],
          'name' => "",
          'type' => $gokbType
        };

        ## Name

        $tipp{'title'}{'name'} = $titleInfo{'name'};

        ## Identifiers

        foreach my $idPair ( @{ $titleInfo{'identifiers'} } ){
          my %idPair = %{ $idPair };
          push @{ $tipp{'title'}{'identifiers'} } , \%idPair;
        }

        push @{ $package{'tipps'} } , \%tipp;
      } # End TIPP

      # -------------------- Compile warning files --------------------

      if($noViableUrl == 1){
        $numNoUrl++;
        push @titleWarnings , {'009P0'=> "ZDB-URLs != GVK-URLs?"};
        push @titleWarningsGVK , {'009P0'=> "ZDB-URLs != GVK-URLs?"};
      }
      if(scalar @titleWarnings > 0){
        $authorityNotes{ $knownSelection{$sigel}{'authority'} }{$sigel}{$id} = \@titleWarnings;
      }
      if(scalar @titleWarningsZDB > 0){
        $authorityNotesZDB{ $knownSelection{$sigel}{'authority'} }{$sigel}{$id} = \@titleWarningsZDB;
      }
      if(scalar @titleWarningsGVK > 0){
        $authorityNotesGVK{ $knownSelection{$sigel}{'authority'} }{$sigel}{$id} = \@titleWarningsGVK;
      }

      $titlesTotal++;

      # -------------------- Collect IDs --------------------

      if(!$allIDs{$id}){
        $allIDs{$id} = {
            'title' => $titleInfo{'name'},
            'connected' => \@relatedPrev
        };
        unless($globalIDs{$id}){
          $globalIDs{$id} = { 'eissn' => $eissn };
        }
        push @allTitles , \%titleInfo;
      }else{
        say "ID ".$id." ist bereits in der Titelliste vorhanden!";
      }
    } ## End TitleInstance

    if($filter){
      say $out_pkg $json_pkg->pretty(1)->encode( \%package );
    }
    if($postData == 1){
      say "Submitting Package $sigel to GOKb (".$gokbCreds{'base'}.")";
      my $postResult = postData('crossReferencePackage', \%package);
      if($postResult != 0){
        say "Could not Upload Package $sigel! Errorcode $postResult";
        $skippedPackages .= $sigel." ";
      }
    }
    $packagesTotal++;
    say "Finished processing $currentTitle Titles of package $sigel.";
  } ## End Package

  # Write collected warnings to file

  say $out_warnings $json_warning->pretty(1)->encode( \%authorityNotes );
  say $out_warnings_zdb $json_warning_zdb->pretty(1)->encode( \%authorityNotesZDB );
  say $out_warnings_gvk $json_warning_gvk->pretty(1)->encode( \%authorityNotesGVK );

  # Write collected titles to file

  if($filter){
    my $tfile = $titleDir->file("titles_$filter.json");
    $tfile->touch();
    my $out_titles = $tfile->openw();
    say $out_titles $json_titles->pretty(1)->encode( \@allTitles );
    close($out_titles);
  }

  # Submit collected titles to GOKb

  my $skippedTitles = 0;

  if($postData == 1){
    sleep 3;
    say "Submitting Titles to GOKb (".$gokbCreds{'base'}.")";
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

  say "\n**********************\n";
  say "Runtime: $timeElapsed";
  say "$titlesTotal relevante Titel in $packagesTotal Paketen";
  say "$numNoUrl Titel ohne NL-URL";
  say "$duplicateISSNs ZDB-ID-Änderungen ohne ISSN-Anpassung";
  say "$wrongISSN eISSNs als Parallel-ISSN (005P0)";
  say "$noPubGiven Titel ohne Verlag (033An)";
  say "$noPubMatch Verlagsfelder mit der GOKb unbekanntem Verlagsnamen (033An)";
  say "$pubFromAuthor als Verlag verwendete Autoren (021Ah)";
  say "$pubFromCorp als Verlag verwendete Primärkörperschaften (029Aa)";
  if($skippedPackages ne ""){
    say "Wegen Fehler beim Upload übersprungene Pakete: $skippedPackages";
  }
  if($skippedTitles != 0){
    say "Anzahl wegen Fehler beim Upload übersprungene Titel: $skippedTitles";
  }
  say "\n**********************\n";
}

# Submit package/title JSON to GOKb-API

sub postData {
  my $endPointType = $_[0];
  my $data = $_[1];
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
  my ($issn) = $_[0];
  if($issn && $issn =~ /^[0-9xX]{4}-?[0-9xX]{4}$/){
    if(index($issn, '-') eq '-1'){
      $issn = join('-', unpack('a4 a4', $issn));
    }
    $issn =~ s/x/X/g;
    return $issn;
  }else{
    return "";
  }
}

# ensure ZDB-ID format

sub formatZdbId {
  my ($id) = $_[0];

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
  my $pubName = $_[0];
  my $normPubName = normalizeString($pubName);
  my $publisherMatch = 0;
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
  return $publisherMatch;
}

# Normalize String
# note: replicated GOKb string normalization process

sub normalizeString {
  my $origString = $_[0];
  my $normString = "";
  my @stopWords = ( "and", "the", "from" );
  my $NFD_string = NFD($origString);
  $NFD_string =~ s/\\p\{InCombiningDiacriticalMarks\}\+/ /g;
  $NFD_string = lc($NFD_string);
  my @stringParts = split(/\s/, $NFD_string);
  @stringParts = sort @stringParts;
  foreach my $stringPart (@stringParts){
    unless($stringPart ~~ @stopWords){
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
  my @parts = @_;
  my @combined = ("","");
  my $i = 0;
  for($i; $i <= 2; $i++){
    my $startDatePart = $parts[0][$i];
    if(!looks_like_number($startDatePart)){
      $startDatePart = substr($startDatePart, 0, 4);
    }
    if($startDatePart != 0){
      if($i != 0){
        $combined[0] .= "-";
      }
      $combined[0] .= $startDatePart;
    }
  }
  for($i; $i <= 5; $i++){
    my $endDatePart = $parts[0][$i];
    if(!looks_like_number($endDatePart)){
      $endDatePart = substr($endDatePart, 0, 4);
    }
    if($endDatePart != 0){
      if($i != 3){
        $combined[1] .= "-";
      }
      $combined[1] .= $endDatePart;
    }
  }
  return \@combined;
}
