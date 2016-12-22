#!/usr/bin/perl

# Dieses Script dient der Synchronisation von Nationallizenz-Paketen mit der GOKb
# Es werden (je nach Aufruf) mehrere Schritte durchlaufen:
# 1. Import der Sigelinformationen aus dem Nationallizenzen-CMS (benötigt Login als Parameter)
# 2. Anreicherung der Paketinformationen mit Daten aus dem ZDB-Sigelverzeichnis
# 3. Extrahieren von Titelinformationen über die SRU-Schnittstelle des GBV
# 4. Upload der Paket- und Titeldaten in eine GOKb-Instanz (nach Login wird gefragt)
#
# Parameter:
# --packages "data_source,username,password" <- erstellt die known_seals.json
# --tsv (ZDB-1-...) <- alte Methode, known_seals.json muss vorhanden sein. Ohne folgendes Paketsigel werden alle Pakete bearbeitet.
# --json (ZDB-1-...) <- neue Methode, known_seals.json muss vorhanden sein. Ohne folgendes Paketsigel werden alle Pakete bearbeitet.
# --post (URL) <- Folgt keine URL, wird die localhost Standardadresse verwendet. Nur zulässig nach --json

use strict;
use warnings;
use utf8;
use DBI;
use JSON;
use URI;
use Unicode::Normalize;
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


my $packageDir = dir("packages");
my $titleDir = dir("titles");
my $warningDir = dir("warnings");

my $knownSeals = 'known_seals.json'; # JSON-Datei mit Paketinformationen
my $baseUrl = 'http://localhost:8080/gokb/'; # URL der Ziel-GOKb
my $ncsu_orgs = do {              # JSON-Datei mit GOKb-Organisationsdaten
  open(my $orgs_in, '<' , "ONLD.jsonld")
      or die("Can't open ONLD.jsonld: $!\n");
  local $/;
  <$orgs_in>
};
my %orgsJSON = %{decode_json($ncsu_orgs)} or die "Konnte JSON mit NCSU-Orgs nicht dekodieren! \n";

# Handle parameters

my $gokbUser;
my $gokbPw;

my $argP = first_index { $_ eq '--packages' } @ARGV;
my $argT = first_index { $_ eq '--tsv' } @ARGV;
my $argJ = first_index { $_ eq '--json' } @ARGV;
my $argPost = first_index { $_ eq '--post' } @ARGV;

if(index($ARGV[$argPost+1], "http") == 0){
  $baseUrl = $ARGV[$argPost+1];
}

if($argP >= 0){
  if(index($ARGV[$argP+1], "dbi") == 0){
    my @creds = split(",", $ARGV[$argP+1]);
    if(scalar @creds == 3){
      if($argT >= 0){
        if(getSeals($creds[0],$creds[1],$creds[2]) == 0){
          if(index($ARGV[$argT+1], "ZDB") == 0){
            createTSV($ARGV[$argT+1]);
          }else{
            print "Pakete abgerufen, erstelle TSVs!\n";
            createTSV();
          }
        }else{
          print "Erstelle keine TSVs, Sigeldatei wurde nicht erstellt!\n";
        }
      }elsif($argJ >= 0){
        if(getSeals($creds[0],$creds[1],$creds[2]) == 0){
          my $post = 0;
          if($argPost >= 0){
            print "GOKb-Benutzername:\n";
            $gokbUser = <STDIN>;
            print "GOKb-Passwort:\n";
            ReadMode 2;
            $gokbPw = <STDIN>;
            ReadMode 0;
            if($gokbUser && $gokbPw){
              $post = 1;
            }else{
              print "Kein Benutzername/Passwort eingegeben, überspringe GOKb-Import!\n";
            }
          }
          if(index($ARGV[$argJ+1], "ZDB") == 0){
            createJSON($post, $ARGV[$argJ+1]);
          }else{
            print "Pakete abgerufen, erstelle JSONs!\n";
            createJSON($post);
          }
        }else{
          print "Erstelle keine JSONs, Sigeldatei wurde nicht erstellt!\n";
        }
      }else{
          print "Erstelle nur Paketdatei!\n";
          getSeals($creds[0],$creds[1],$creds[2]);
      }
    }else{
        print "Falsches Format der DB-Daten! Abbruch!\n";
    }
  }else{
      print "Datenbankinformationen fehlen/falsch! Format ist: \"data_source,username,password\"\n"
  }
}elsif($argT >= 0){
  if(-e $knownSeals){
    if($ARGV[$argT+1] && index($ARGV[$argT+1], "ZDB") == 0){
      my $filterSigel = $ARGV[$argT+1];
      print "Paketdatei gefunden, erstelle TSV für $filterSigel!\n";
      createTSV($filterSigel);
    }else{
      print "Paketdatei gefunden, erstelle TSVs!\n";
      createTSV();
    }
  }else{
    print "Paketdatei nicht vorhanden! Zum Erstellen mit Parameter '--packages' starten!\n";
  }
}elsif($argJ >= 0){
  if(-e $knownSeals){
    my $post = 0;
    if($argPost >= 0){
      print "GOKb-Benutzername:\n";
      $gokbUser = <STDIN>;
      chomp $gokbUser;
      ReadMode 2;
      print "GOKb-Passwort:\n";
      $gokbPw = <STDIN>;
      ReadMode 0;
      chomp $gokbPw;
      if($gokbUser && $gokbPw){
        $post = 1;
      }else{
        print "Kein Benutzername/Passwort eingegeben, überspringe GOKb-Import!\n";
      }
    }
    if($ARGV[$argJ+1] && index($ARGV[$argJ+1], "ZDB") == 0){
      my $filterSigel = $ARGV[$argJ+1];
      print "Paketdatei gefunden, erstelle JSON für $filterSigel!\n";
      createJSON($post, $filterSigel);
    }else{
      print "Paketdatei gefunden, erstelle JSONs!\n";
      createJSON($post);
    }
  }else{
    print "Paketdatei nicht vorhanden! Zum Erstellen mit Parameter '--packages' starten!\n";
  }
}

# No parameters

if(scalar @ARGV == 0){

  print "Mögliche Parameter sind '--packages', sowie '--json' (aktuell) oder '--tsv' (alte Methode). Falls beide vorhanden sind, werden sie in dieser Reihenfolge bearbeitet.\n";
  print "Wechsel zu interaktivem Modus..\n";
  print "Bitte Aufgabe eingeben (1: Aktuelle Sigel abrufen; 2: JSON vorhanden, nur TSV erstellen; 3: Neue JSON-Methode verwenden; 4: Paket-JSON erstellen und zur GOKb hochladen):\n";
  my $selected = <>;

  if($selected == 1){
    getSeals();
  }elsif($selected == 2){
    createTSV();
  }elsif($selected == 3){
    createJSON(0);
  }elsif($selected == 4){
    print "GOKb-Benutzername:\n";
    $gokbUser = <STDIN>;
    chomp $gokbUser;
    ReadMode 2;
    print "GOKb-Passwort:\n";
    $gokbPw = <STDIN>;
    ReadMode 0;
    chomp $gokbPw;
    if($gokbUser && $gokbPw){
      createJSON(1);
    }else{
      print "Kein Benutzername/Passwort eingegeben, beende Programm!\n";
    }
  }else{
    print "Keine valide Option gewählt. Beende Programm!\n";
  }
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

# Create title lists from package metadata
# !OLD method, now using createJSON()

sub createTSV {
  my $filter = $_[0];
  my $json_text = do {
    open(my $json_fh, '<' , $knownSeals)
        or die("Can't open \$filename\": $!\n");
    local $/;
    <$json_fh>
  };
  my $titlesTotal = 0;
  my $packagesTotal = 0;
  my $json = JSON->new;
  my %known = %{decode_json($json_text)} or die "JSON nicht vorhanden! \n";
  my %knownSelection;
  if($filter){
    $knownSelection{$filter} = $known{$filter};
    print "Creating title list for $filter!\n";
  }else{
    %knownSelection = %known;
    print "Creating title lists for all packages!\n"
  }
  foreach my $sigel (keys %knownSelection){
    my @previousIDs;
    my @tableColumns  = (
      "publication_title",
      "print_identifier",
      "online_identifier",
      "date_first_issue_online",
      "num_first_vol_online",
      "num_first_issue_online",
      "date_last_issue_online",
      "num_last_vol_online",
      "num_last_issue_online",
      "title_url",
      "first_author",
      "title_id",
      "embargo_info",
      "coverage_depth",
      "coverage_notes",
      "publisher_name",
      "preceding_publication_title_id",
      "parent_publication_title_id"
    );
    my $currentTitle = 0;
    print "Processing Package ".$sigel."...\n";
    if(scalar @{ $known{$sigel}{'zdbOrgs'} } == 0){
      print "Paket hat keine verknüpften Institutionen in der ZBD. Überspringe Paket.\n";
      next;
    }
    my $ts = strftime "%Y%m%d", localtime;
    my $tsvName = $packageDir->file($sigel.".tsv");
    if (-e $tsvName) {
      copy($tsvName, $packageDir->file($sigel.'_last.tsv'));
      open(my $fh, '>:encoding(UTF-8)', $tsvName) or die "Could not open file '$tsvName' $!";
      close($fh);
    }
    my %attrs = (
      base => 'http://sru.gbv.de/gvk',
      query => 'pica.xpr='.$sigel.' sortBy year/sort.ascending',
      recordSchema => 'picaxml',
      parser => 'picaxml'
    );
    writeLine($sigel, \@tableColumns);
    my $noPublisher = 0;
    my $importer = Catmandu::Importer::SRU->new(%attrs) or die "Abfrage über ".$attrs{'base'}." fehlgeschlagen!\n";

    while (my $titleRecord = $importer->next){
      $currentTitle++;

      # Check, if the package correctly contains journals only

      if(substr(pica_value($titleRecord, '002@0'), 0, 2) ne 'Ob' && $currentTitle <= 2){
        print "Überspringe Paket ".$sigel.": Nicht-Zeitschrift in den ersten 2 Treffern: ".pica_value($titleRecord, '002@0')."\n";
        unlink $tsvName or warn "Konnte Titelliste für ".$sigel." nicht löschen!\n";
        last;
      }elsif(substr(pica_value($titleRecord, '002@0'), 0, 2) ne 'Ob'){
        print "Überspringe Titel ".pica_value($titleRecord, '021Aa').", Materialcode: ".pica_value($titleRecord, '002@0')."\n";
        next;
      }

      # Collect date fragments for processing
      my $start_year = 0;
      if(pica_value($titleRecord, '031Nj')){
        $start_year = pica_value($titleRecord, '031Nj');
      }elsif(pica_value($titleRecord, '011@a')){
        $start_year = pica_value($titleRecord, '011@a');
      }
      my $end_year = 0;
      if(pica_value($titleRecord, '031Nk')){
        $end_year = pica_value($titleRecord, '031Nk');
      }elsif(pica_value($titleRecord, '011@b')){
        $end_year = pica_value($titleRecord, '011@b');
      }

      my @dates = (
        $start_year,
        pica_value($titleRecord, '031Nc') ? pica_value($titleRecord, '031Nc') : 0,
        pica_value($titleRecord, '031Nb') ? pica_value($titleRecord, '031Nb') : 0,
        $end_year,
        pica_value($titleRecord, '031Nm') ? pica_value($titleRecord, '031Nm') : 0,
        pica_value($titleRecord, '031Nl') ? pica_value($titleRecord, '031Nl') : 0
      );

      my @dts = transformDate(\@dates);

      # Prepare KBART fields

      my $title = "";
      my $issn = "";
      my $eissn = "";
      my $fromTime = $dts[0][0];
      my $firstVol = pica_value($titleRecord, '031Nd') ? pica_value($titleRecord, '031Nd') : "";
      my $firstIssue = pica_value($titleRecord, '031Ne') ? pica_value($titleRecord, '031Ne') : "";
      my $toTime = $dts[0][1];
      my $lastVol = pica_value($titleRecord, '031Nn') ? pica_value($titleRecord, '031Nn') : "";
      my $lastIssue = pica_value($titleRecord, '031No') ? pica_value($titleRecord, '031No') : "";
      my $id = "";
      my $url = "";
      my $embargo = "";
      my $publisher = "";
      my $author = "";
      my $coverage_depth = "";
      my $coverage_notes = "";
      my $preceding_id = "";
      my $parent_id = "";

      # Format ISSN(s)

#       if(pica_value($titleRecord, '005P0')){
#         my $unformatedIssn = pica_value($titleRecord, '005P0');
#         if(index($unformatedIssn, '-') eq '-1'){
#           $issn = join('-', unpack('a4 a4', $unformatedIssn));
#         }
#       }

      if(pica_value($titleRecord, '005A0')){
        my $unformatedIssn = pica_value($titleRecord, '005A0');
        if(index($unformatedIssn, '-') eq '-1'){
          $eissn = join('-', unpack('a4 a4', $unformatedIssn));
          $eissn =~ s/x/X/g;
        }
      }

      # Preceeding Title

      my @relatedIDs = pica_values($titleRecord, '039E6');
      my $relIDsNum = (scalar @relatedIDs)/2;

      my @relatedComments = pica_values($titleRecord, '039Ec');

      my $relComNum = scalar @relatedComments;
      my $relPos = 0;
      if($relIDsNum == $relComNum){
        foreach my $relatedComment (@relatedComments){
          $relatedIDs[$relPos*2] =~ s/-//g;
          $relatedIDs[$relPos*2] =~ s/x/X/g;
          if(index($relatedComment, "Vorg.") == 0 || $relatedComment eq "Darin aufgeg." || $relatedComment eq "Fortsetzung von"){
            $preceding_id = $relatedIDs[$relPos*2];
          }
          $relPos++;
        }
      }

      # Get correct URL

      my @sourcePlatforms = pica_values($titleRecord, '009P[05]x');
      my @sourceURLS = pica_values($titleRecord, '009P[05]a');
      my $sourcePos = 0;
      foreach my $platform (@sourcePlatforms){
        if($platform eq 'Verlag' && length($sourceURLS[$sourcePos]) <= 255){
          $url = $sourceURLS[$sourcePos];
        }
        $sourcePos++;
      }

      ## Fallback 1: Check for any continuing publisher sources

      $sourcePos = 0;
      my %possibleSources;
      if($url eq ""){
        foreach my $platform (@sourcePlatforms){
          if(length($sourceURLS[$sourcePos]) <= 255){
            if(index($platform, 'Verlag') == 0 && substr($platform, -1) eq '-'){
              $platform = substr($platform, 0, (length $platform)-1);
              if(substr($platform, -2) eq ' '){
                substr($platform, -2, 2, "");
              }
              if(rindex($platform, ',') <= 5){
                my $cPos = rindex($platform, ',');
                $platform = substr($platform, 0, $cPos);
              }
              if(looks_like_number(substr($platform, -4, 4))){
                $possibleSources{$sourcePos} = substr($platform, -4, 4);
              }elsif(looks_like_number(substr($platform, -7, 4))){
                $possibleSources{$sourcePos} = substr($platform, -7, 4);
              }
            }
          }
          $sourcePos++;
        }
        if(scalar keys %possibleSources > 0){
          my @sortedSources = sort { $possibleSources{$a} <=> $possibleSources{$b} } keys %possibleSources;
          if(%possibleSources){
            $url = $sourceURLS[$sortedSources[0]];
          }
        }
      }

      ## Fallback 2: Check for other publisher sources

      $sourcePos = 0;
      if($url eq ""){
        foreach my $platform (@sourcePlatforms){
          if(index($platform, 'Verlag') >= 0){
            if(length($sourceURLS[$sourcePos]) <= 255){
              $url = $sourceURLS[$sourcePos];
            }
          }
          $sourcePos++;
        }
      }

      ## Fallback 3: Check for digitalisation sources

      $sourcePos = 0;
      if($url eq ""){
        foreach my $platform (@sourcePlatforms){
          if($platform eq 'Digitalisierung'){
            if(length($sourceURLS[$sourcePos]) <= 255){
              $url = $sourceURLS[$sourcePos];
            }
          }
          $sourcePos++;
        }
      }
      ## Fallback 4: Select EZB source

      $sourcePos = 0;
      if($url eq ""){
        foreach my $platform (@sourcePlatforms){
          if(index($platform, 'EZB') == 0){
            $url = $sourceURLS[$sourcePos];
          }
          $sourcePos++;
        }
      }

      # Title (publication_title)

      if(pica_value($titleRecord, '025@a')){
        my $titleField = pica_value($titleRecord, '025@a');
        if(index($titleField, '@') <= 5){
          $titleField =~ s/@//;
        }
        $title = $titleField;
      }elsif(pica_value($titleRecord, '021Aa')){
        my $titleField = pica_value($titleRecord, '021Aa');
        if(index($titleField, '@') <= 5){
          $titleField =~ s/@//;
        }
        $title = $titleField;
      }

      # Publisher

      if(pica_value($titleRecord, '033An')){
        $publisher = pica_value($titleRecord, '033An');
      }

      # ZDB-ID

      my @zdbIDs = pica_values($titleRecord, '006Z0');
      foreach my $zdbID (@zdbIDs){
        $zdbID =~ s/-//g;
        if($zdbID =~ /^\d*[xX]?$/){
          $zdbID =~ s/x/X/g;
          $id = $zdbID;
        }
      }

      # Finished table row

      my @titleRow = (
          $title,
          $issn,
          $eissn,
          $fromTime,
          $firstVol,
          $firstIssue,
          $toTime,
          $lastVol,
          $lastIssue,
          $url,
          $author,
          $id,
          $embargo,
          $coverage_depth,
          $coverage_notes,
          $publisher,
          $preceding_id,
          $parent_id
      );
#       if($id ~~ @previousIDs){
#         print "Titel mit ZDB-ID $id wurde bereits geschrieben!\n";
#       }else{
        print "Schreibe Titel ".$currentTitle." von Paket ".$sigel." (".$title.")\n";
        writeLine($sigel, \@titleRow);
        $titlesTotal++;
#       }
#       push @previousIDs, $id;
    }
  };
  print $packagesTotal." Pakete mit ".$titlesTotal." Zeitschriftentiteln verarbeitet.\n";
  return 0;
}

# Create packages, tipps and titles as GOKb-JSON (and trigger upload if requested)

sub createJSON {

  my $postData = $_[0];
  my ($filter) = $_[1];

  my $json_text = do {
    open(my $json_fh, '<' , $knownSeals)
        or die("Can't open \$filename\": $!\n");
    local $/;
    <$json_fh>
  };
  $packageDir->mkpath( { verbose => 0 } );
  $titleDir->mkpath( { verbose => 0 } );
  $warningDir->mkpath( { verbose => 0 } );

  my $out_warnings;
  my $out_warnings_zdb;
  my $out_warnings_gvk;

  if(!$filter){
    my $wfile = $warningDir->file("Warnings_all.json");
    $out_warnings = $wfile->touch()->openw();
    $out_warnings_zdb = $warningDir->file("Warnings_zdb_all.json")->touch()->openw();
    $out_warnings_gvk = $warningDir->file("Warnings_gvk_all.json")->touch()->openw();
  }else{
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

  # Input JSON handling

  my %known = %{decode_json($json_text)} or die "JSON nicht vorhanden! \n";
  my %knownSelection;
  if($filter){
    $knownSelection{$filter} = $known{$filter};
    print "Generating JSON only for $filter!\n";
  }else{
    %knownSelection = %known;
    print "Generating JSON for all packages!\n";
  }
  my $startTime = time();



  ################ PACKAGE ################



  foreach my $sigel (keys %knownSelection){

    my $currentTitle = 0;
    my $noPublisher = 0;
    my %allISSN;
    my %allIDs;
    my %alljson;

    my $json_out = JSON->new->utf8->canonical;
    my $out_pkg;
    if($filter){
      if(-e "$sigel.json"){
        copy("$sigel.json", $sigel."_last.json");
      }
      my $pfile = $packageDir->file("$sigel.json");
      $pfile->touch();
      $out_pkg = $pfile->openw();
    }

    print "Processing Package ".($packagesTotal + 1).", ".$sigel."...\n";
    if(scalar @{ $knownSelection{$sigel}{'zdbOrgs'} } == 0){
      print "Paket hat keine verknüpften Institutionen in der ZBD. Überspringe Paket.\n";
      next;
    }

    ## Package Header

    my $provider = $knownSelection{$sigel}{'provider'};
    my $pkgName = $knownSelection{$sigel}{'name'};
    $pkgName =~ s/:\s//g;
    my $pkgYear = strftime '%Y', localtime;
    $alljson{'packageHeader'} = {
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
      global => "Global",
      listVerifier => "",
      userListVerifier => "",
      nominalPlatform => $knownSelection{$sigel}{'platformURL'} ? $knownSelection{$sigel}{'platformURL'} : "",
      nominalProvider => $provider,
      listVerifiedDate => "",
      source => {
          url => "http://sru.gbv.de/gvk",
          name => "GVK-SRU",
          normname => "GVK_SRU"
      },
      curatoryGroups => [{
        curatoryGroup => "LAS:eR"
      }],
    };

    $alljson{'tipps'} = [];

    my %attrs = (
      base => 'http://sru.gbv.de/gvk',
      query => 'pica.xpr='.$sigel.' and (pica.mak=Obvz or pica.mak=Obv) sortBy year/sort.ascending',
      recordSchema => 'picaxml',
      parser => 'picaxml',
      _max_results => 5
    );

    my $sruTitles = Catmandu::Importer::SRU->new(%attrs) or die "Abfrage über ".$attrs{'base'}." fehlgeschlagen!\n";



    ################ TITLEINSTANCE ################



    while (my $titleRecord = $sruTitles->next){
      $currentTitle++;
      my %titleInfo;
      my $id;
      my $eissn;
      my @relatedPrev = ();
      my @titleWarnings;
      my @titleWarningsZDB;
      my @titleWarningsGVK;

      # Check, if the title is a journal (shouldn't be necessary since it's included in the search query)

      if(substr(pica_value($titleRecord, '002@0'), 0, 2) ne 'Ob' || pica_value($titleRecord, '003@0') eq '510617395'){
        print "Überspringe Titel ".pica_value($titleRecord, '021Aa').", Materialcode: ".pica_value($titleRecord, '002@0')."\n";
        next;
      }

      print "Verarbeite Titel ".$currentTitle." von Paket ".$sigel."(".pica_value($titleRecord, '006Z0').")\n";

      # -------------------- Identifiers --------------------

      $titleInfo{'identifiers'} = [];

      ## ZDB-ID

      if(pica_value($titleRecord, '006Z0')){
        my @zdbIDs = pica_values($titleRecord, '006Z0');
        foreach my $zdbID (@zdbIDs){
          $zdbID =~ s/-//g;
          if($zdbID =~ /^\d*[xX]?$/){
            $zdbID =~ s/x/X/g;
            substr($zdbID, -1, 0, '-');
            $id = $zdbID;
          }
        }
        if($id){
          push @{ $titleInfo{'identifiers'} } , { 'type' => "zdb", 'value' => $id };
        }
      }else{
        print "Titel mit ppn ".pica_value($titleRecord, '003@0')." hat keine ZDB-ID! Überspringe Titel..\n";
        push @titleWarnings, { '006Z0' => pica_values($titleRecord, '006Z0') };
        push @titleWarningsGVK, { '006Z0' => pica_values($titleRecord, '006Z0') };
        next;
      }

      ## eISSN

      if(pica_value($titleRecord, '005A0')){
        my $unformatedIssn = pica_value($titleRecord, '005A0');
        if(index($unformatedIssn, '-') eq '-1'){
          $eissn = join('-', unpack('a4 a4', $unformatedIssn));
          $eissn =~ s/x/X/g;
          if($allISSN{$eissn} || ( $globalIDs{$id} && $globalIDs{$id}{'eissn'} ne $eissn ) ){
            print "ISSN wurde bereits vergeben!\n";
            $duplicateISSNs++;
            push @titleWarnings, { '005A0' => $eissn , 'comment' => 'gleiche eISSN nach Titeländerung?' };
            push @titleWarningsZDB, { '005A0' => $eissn , 'comment' => 'gleiche eISSN nach Titeländerung?' };
          }else{
            push @{ $titleInfo{'identifiers'}} , { 'type' => "eissn", 'value' => $eissn };
            $allISSN{$eissn} = pica_value($titleRecord, '021Aa');
          }
        }
      }

      ## pISSN

      if(pica_value($titleRecord, '005P0')){
        my $unformatedIssn = pica_value($titleRecord, '005P0');
        if(index($unformatedIssn, '-') eq '-1'){
          my $pissn = join('-', unpack('a4 a4', $unformatedIssn));
          $pissn =~ s/x/X/g;
          if($allISSN{$pissn}){
            print "Print-ISSN wurde bereits als eISSN vergeben!\n";
            $wrongISSN++;
            push @titleWarnings, { '005P0' => $pissn , 'comment' => 'gleiche Vorgänger-eISSN als Parallel-ISSN?' };
            push @titleWarningsZDB, { '005P0' => $pissn , 'comment' => 'gleiche Vorgänger-eISSN als Parallel-ISSN?' };
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
        print "Keinen Titel für ".pica_value($titleRecord, '006Z0')." erkannt, überspringe Titel!\n";
        push @titleWarnings, { '021Aa' => pica_value($titleRecord, '021Aa') };
        push @titleWarningsZDB, { '021Aa' => pica_value($titleRecord, '021Aa') };
        next;
      }

      # -------------------- Other GOKb Fields --------------------

      $titleInfo{'type'} = "Serial";
      $titleInfo{'status'} = "Current";
      $titleInfo{'editStatus'} = "In Progress";
      $titleInfo{'shortcode'} = "";
      $titleInfo{'medium'} = "Journal";
      $titleInfo{'defaultAccessURL'} = "";
      $titleInfo{'OAStatus'} = "";
      $titleInfo{'issuer'} = "";
      $titleInfo{'imprint'} = "";
      $titleInfo{'continuingSeries'} = "";

      # -------------------- Release notes --------------------

      my @releaseNotes = @{ pica_fields($titleRecord, '031N')};
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
          my $ncsuPub = searchNcsuOrgs($tempPub);
          if($ncsuPub ne '0'){
            push @{ $titleInfo{'publisher_history'}} , {
                'name' => $ncsuPub,
                'startDate' => $pubStart ? $pubStart : "",
                'endDate' => $pubEnd ? $pubEnd : "",
                'status' => $pubEnd && $pubEnd ne convertToTimeStamp($dts[0][1], 1) ? "Former" : "Active"
            };
          }elsif($ncsuPub eq '0' || $tempPub =~ /[\[\]]/ || $tempPub =~ /u\.\s?a\./){
            $noPubMatch++;
            push @titleWarnings, { '033An' => $preCorrectedPub };
            push @titleWarningsZDB, { '033An' => $preCorrectedPub };
          }
        }
      }

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
            my $oID = $relTitle[$subfPos+2];
            $oID =~ s/-//g;
            if($oID =~ /^\d*[xX]?$/){
              $oID =~ s/x/X/g;
              substr($oID, -1, 0, '-');
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
                # push @titleWarnings, { '039Ed' => $relTitle[$subfPos+1] };
              }
            }
            if($tempStartYear){
              $relatedStartYear = $tempStartYear;
              if($subField eq 'd') {
                # push @titleWarnings, { '039Ed' => $relTitle[$subfPos+1] };
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
                      'identifiers' => [ { 'type' => "zdb", 'value' => $relatedID } ]
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
                          'identifiers' => [ { 'type' => "zdb", 'value' => $relatedID } ]
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
                          'identifiers' => [ { 'type' => "zdb", 'value' => $relatedID } ]
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
                        'identifiers' => [ { 'type' => "zdb", 'value' => $relatedID } ]
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
                      'identifiers' => [ { 'type' => "zdb", 'value' => $relatedID } ]
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
                        'identifiers' => [ { 'type' => "zdb", 'value' => $relatedID } ]
                    }],
                    'to' => [{
                        'title' => $titleInfo{'name'},
                        'identifiers' => $titleInfo{'identifiers'}
                    }]
                };
              }
            }else{
              print "Konnte keinen direkten Vorgänger bzw. Nachfolger ausmachen für Daten $start_year-$end_year und $relatedStartYear-".($relatedEndYear ? $relatedEndYear : "")."\n";
            }
          }
        }
      }

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
          }elsif($subField eq 'x'){
            $internalComments = $eSource[$subfPos+1];
          }elsif($subField eq 'z'){
            $publicComments = $eSource[$subfPos+1];
          }
          $subfPos++;
        }

        if(!$sourceURL || length $sourceURL > 255 || $sourceURL eq ""){
          print "Skipping overlong URL!\n";
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
          "Looks like a wrong URL >".pica_value($titleRecord, '006Z0')."\n";
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

        # -------------------- TitleInstance --------------------

        $tipp{'title'} = {
          'identifiers' => [],
          'name' => "",
          'type' => 'Serial'
        };

        ## Name

        $tipp{'title'}{'name'} = $titleInfo{'name'};

        ## Identifiers

        foreach my $idPair ( @{ $titleInfo{'identifiers'} } ){
          my %idPair = %{ $idPair };
          push @{ $tipp{'title'}{'identifiers'} } , \%idPair;
        }

        push @{ $alljson{'tipps'} } , \%tipp;
      }
      if($noViableUrl == 1){
        $numNoUrl++;
        push @titleWarnings , {'009P0'=> "ZDB-URLs != GVK-URLs?"};
        push @titleWarningsGVK , {'009P0'=> "ZDB-URLs != GVK-URLs?"};
      }
      if(scalar @titleWarnings > 0){
        $authorityNotes{ $knownSelection{$sigel}{'authority'} }{$sigel}{ pica_value($titleRecord, '006Z0') } = \@titleWarnings;
      }
      if(scalar @titleWarningsZDB > 0){
        $authorityNotesZDB{ $knownSelection{$sigel}{'authority'} }{$sigel}{ pica_value($titleRecord, '006Z0') } = \@titleWarningsZDB;
      }
      if(scalar @titleWarningsGVK > 0){
        $authorityNotesGVK{ $knownSelection{$sigel}{'authority'} }{$sigel}{ pica_value($titleRecord, '006Z0') } = \@titleWarningsGVK;
      }
      $titlesTotal++;
      if(!$allIDs{$id}){
        $allIDs{$id} = { 'title' => $titleInfo{'name'}, 'connected' => \@relatedPrev };
        unless($globalIDs{$id}){
          $globalIDs{$id} = { 'eissn' => $eissn };
        }
        push @allTitles , \%titleInfo;
      }else{
        print "ID ".$id." mehrmals in Titelliste!\n";
      }
    }
    if($filter){
      say $out_pkg $json_out->pretty(1)->encode( \%alljson );
    }
    if($postData == 1){
      print "Submitting Package $sigel to GOKb..\n";
      my $postResult = postData('crossReferencePackage', \%alljson);
      if($postResult != 0){
        print "Could not Upload Package $sigel! Errorcode $postResult\n";
        $skippedPackages .= $sigel." ";
      }
    }
    $packagesTotal++;
  }

  say $out_warnings $json_warning->pretty(1)->encode( \%authorityNotes );
  say $out_warnings_zdb $json_warning_zdb->pretty(1)->encode( \%authorityNotesZDB );
  say $out_warnings_gvk $json_warning_gvk->pretty(1)->encode( \%authorityNotesGVK );

  if($filter){
    my $tfile = $titleDir->file("titles_$filter.json");
    $tfile->touch();
    my $out_titles = $tfile->openw();
    say $out_titles $json_titles->pretty(1)->encode( \@allTitles );
    close($out_titles);
  }
  my $skippedTitles = 0;
  if($postData == 1){
    sleep 3;
    print "Submitting Titles to GOKb..\n";
    foreach my $title (@allTitles){
      my %curTitle = %{ $title };
      my $postResult = postData('crossReferenceTitle', \%curTitle);
      if($postResult != 0){
        print "Could not upload Title! Errorcode $postResult\n";
        $skippedTitles++;
      }
    }
  }
  my $timeElapsed = duration(time() - $startTime);
  print "\n**********************\n\n";
  print "Runtime: $timeElapsed \n";
  print "$titlesTotal relevante Titel in $packagesTotal Paketen\n";
  print "$numNoUrl Titel ohne NL-URL\n";
  print "$duplicateISSNs ZDB-ID-Änderungen ohne ISSN-Anpassung\n";
  print "$wrongISSN eISSNs als Parallel-ISSN (005P0)\n";
  print "$noPubGiven Titel ohne Verlag (033An)\n";
  print "$noPubMatch Verlagsfelder mit der GOKb unbekanntem Verlagsnamen (033An)\n";
  print "$pubFromAuthor als Verlag verwendete Autoren (021Ah)\n";
  print "$pubFromAuthor als Verlag verwendete Primärkörperschaften (029Aa)\n";
  if($skippedPackages ne ""){
    print "Wegen Fehler beim Upload übersprungene Pakete: $skippedPackages \n";
  }
  if($skippedTitles != 0){
    print "Anzahl wegen Fehler beim Upload übersprungene Titel: $skippedTitles \n";
  }
  print "\n**********************\n\n";
}

# Submit package/title JSON to GOKb-API

sub postData {
  my $endPointType = $_[0];
  my $data = $_[1];
  my $endPoint = $baseUrl."integration/".$endPointType;
  
  if($data && ref($data) eq 'HASH'){
  
    my $json_out = JSON->new->utf8->canonical;
    my %decData = %{ $data };  
    my $ua = LWP::UserAgent->new;
    $ua->timeout(1800);
    my $req = HTTP::Request->new(POST => $endPoint);
    $req->header('content-type' => 'application/json');
    $req->authorization_basic($gokbUser, $gokbPw);
    $req->content($json_out->encode( \%decData ));
    
    my $resp = $ua->request($req);
    if($resp->is_success){
      if($endPointType eq 'crossReferencePackage'){
        print "Commit of package successful.\n";
      }
      return 0;
      
    }else{
      print "HTTP POST error code: ", $resp->code, "\n";
      print "HTTP POST error message: ", $resp->message, "\n";
      return $resp->code;
    }
  }else{
    print "Wrong endpoint or no data!\n";
    return -1;
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

# Write title to TSV

sub writeLine {
  my $tsvSigel = $_[0];
  my $i = 0;
  my @columns = $_[1];
  my $file = $packageDir->file($tsvSigel.".tsv");
  my $fh = $file->opena();
  foreach my $column (@{$columns[0]}){
    $i++;
    print $fh $column;
    if($i < scalar @{$columns[0]}){
      print $fh "\t";
    }
  }
  print $fh "\n";
  close($fh);
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


