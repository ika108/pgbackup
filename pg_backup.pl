#! /usr/bin/perl -w

# Load additional library
use strict;
use Getopt::Std;
use pg_notify;
use pg_read;
use pg_write;

our $VERSION = 0.9;

my @notify_rcpt = qw(git@nimporteou.net);
my $notifier = pg_notify->new();

&run();

sub run {
    if(grep(/-v/,@ARGV)){
    	$notifier->verbosity('DEBUG');
        $notifier->notify('VERBOSE');
    }
    $notifier->notify('START');
    my $params = &get_params();
    $notifier->params($params);
    if($params->{'h'}){&HELP_MESSAGE; $notifier->notify('END'); exit(0);}

    my $reader = pg_read->new($notifier,$params);
    my $writer = pg_write->new($notifier,$params);
    
    $SIG{INT} = sub {
    	$notifier->notify('INT');
    	$reader->end();
    	$writer->end();
    	$notifier->report()
	};

	$reader->buffer($writer->buffer());
    $reader->start();
    $writer->start();

    while($writer->state() ne 'END'){
    	sleep(1);
    }
    
    if($notifier->errorlevel == 1){
    	$notifier->notify('WARNING'); 
    	rename($writer->dumpfile(),$writer->dumpfile()."-WARNING");
    	$writer->dumpfile($writer->dumpfile()."-WARNING");
    } 
    elsif($notifier->errorlevel == 0){
    	$notifier->notify('ERROR'); 
    	rename($writer->dumpfile(),$writer->dumpfile()."-ERROR");
    	$writer->dumpfile($writer->dumpfile()."-ERROR");
    }else{
    	$writer->rotate();
    }
    $notifier->notify('END');
    exit(0);
}

sub get_params {
    $notifier->notify('GETOPTS');
    my %opts;
    getopts('qvhdif:D:kx:', \%opts);
	if($ARGV[$#ARGV]){$opts{'ARGV'} = pop(@ARGV)}
	if($ARGV[$#ARGV]){
    	&HELP_MESSAGE;
    	exit(1);
	}
	my $argstring;
	foreach my $arg (keys(%opts)){
    	$argstring .= "$arg => $opts{$arg}";
	}
	$notifier->notify('ARGV',$argstring);
	return(\%opts);
}

sub HELP_MESSAGE {
    &debug('HELP');
    $Getopt::Std::STANDARD_HELP_VERSION = 1;
    my $fh = shift;
    if(! $fh){$fh = *STDERR}
    print $fh ("Usage : $0 [OPTIONS] [PATH]\n");
    print $fh ("Create a full dump of a postgres database,\n");
    print $fh ("handling dump rotation for long time backup purpose\n");
    print $fh ("  -q\t\tQuiet mode\n");
    print $fh ("  -h\t\tThis help\n");
    print $fh ("  -v\t\tVerbose mode\n");
    print $fh ("  -d\t\tDry run. Don't modify or write anything\n");
    # print $fh ("  -i\t\tForce incremental backup\n");
    print $fh ("  -f\t\tForce full backup\n");
    print $fh ("  -D base\tDatabase to backup (else all)\n");
    print $fh ("  -k\t\tForce to keep previous backup, even if they are expired\n");
    print $fh ("  -F fmt\tBackup file format (plain,custom,tar)\n");
    print $fh ("  -x pgparams\tSpecify pg_dump specific arguments. See man pg_dump\n");
    print $fh ("Report bugs to <git\@nimporteou.net>\n");
}

