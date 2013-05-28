#! /usr/bin/perl -w

# Load additional library
use strict;
use DBI;
use Getopt::Std;
use IPC::Open3;
use IO::Select;
use threads;
use threads::shared;
use Thread::Semaphore;
use Time::HiRes qw(usleep);


our $VERSION = 0.9;

my @notify_rcpt = qw(some_email@toto.com);
my $start_time = time;
my $db_params = 'postgres';
my $table_params = 'public.parameter';
my $pg_service = '/etc/sysconfig/pgsql/pg_service.conf';
my $pg_conf = '/etc/postgresql.conf';
my $dump_dir = '/dump';
my $pg_dump = '/usr/bin/pg_dump';
my $pg_restore = '/usr/bin/pg_restore';
my $pg_blk_size = 128;
my $blk_size = 1024;
my $flush_delay = 100000; # In microsecond = 0.1 second
my $dry_run = 0;
my $format = 'p';
my $pg_params = '';
my $pg_db = '';
my $pg_dumpfile;
my $error_level = 0;


my $DEBUGLEVEL = 3;



$SIG{INT} = sub {
    print('kill int as been called\n');
    exit(254);
};

&run();

sub run {
    if(grep(/-v/,@ARGV)){
        $DEBUGLEVEL = 3;
        &debug(0);
    }
    &debug(1);
    my $params = &get_params();
    if($params->{'h'}){&HELP_MESSAGE}
    if($params->{'d'}){$dry_run = 1}
    if($params->{'i'}){}
    if($params->{'f'}){$format = $params->{'F'}}
    if($params->{'k'}){}
    if($params->{'x'}){$pg_params = $params->{'x'}}
    if($params->{'D'}){
    	$pg_db = $params->{'D'};
    	$pg_dumpfile = "$pg_dump/db-$pg_db-".time;
    }else{
    	$pg_dump .= 'all';
    	$pg_dumpfile = "$pg_dump/db-all-".time;
    }
    my @backup_state = &bck_dump();
    print("ERR LEVEL : $error_level\n");
    &bck_rotate();
    &debug(2);
    exit(0);
}

sub get_params {
    &debug(3);
    my %opts;
    getopts('vhdif:D:kx:', \%opts);
    if($ARGV[$#ARGV]){$opts{'ARGV'} = pop(@ARGV)}
    if($ARGV[$#ARGV]){
        &HELP_MESSAGE;
        exit(1);
    }
    my $argstring = 'ARGV: ';
    foreach my $arg (keys(%opts)){
        $argstring .= "{$arg => $opts{$arg}}";
    }
    &debug('GETOPTS',$argstring);

    if($opts{'ARGV'} and ! (-w $opts{'ARGV'}) ){
        &error
    }
    return(\%opts);
}

sub bck_dump {
    &debug('FULLDUMP');

    # Setting some threaded shared variables
    my $buffer :shared;
    my $errors :shared;
    my $exitcode :shared;
    my $buffer_semaphore = Thread::Semaphore->new();
    my $write_ctrl :shared = 1;
    my $read_ctrl :shared = 1;

    # This is the block for the pg_dump thread
    my $dump_runner = sub {
        &debug('STARTDR',threads->tid());
        my $pgdcmd = "$pg_dump $pg_db --create -F $format $pg_params";
        &debug('CMD',$pgdcmd);
        my $pgdpid;
        # We use open3 instead of open because we need to catch every FH
        eval {
			if($dry_run){
				&debug('DRYDUN',$pgdcmd);
			}else{
            	$pgdpid = open3(\*IPCSTDIN,\*IPCSTDOUT,\*IPCSTDERR,$pgdcmd);
			}
        };
        if($@){
            # open3 has failed, no point going further.
            my $stderr = join('/',<IPCSTDERR>);
            &debug('ERREXEC',$pgdcmd, $@, $stderr);
            close(IPCSTDIN);
            close(IPCSTDOUT);
            close(IPCSTDERR);
        }else{
        	# binmode my not be necessary, but as we process binary data (globs)
        	# better safe than sorry
        	eval{no warnings 'all' ; binmode(IPCSTDOUT)};
        	if($@){
        		if($dry_run){
        			&debug('CAUGHT',$@);
        		}else{
        			&debug('BINMODE',$@);
        		}
        	}
        	&debug('STDIN');
        	eval{close(IPCSTDIN)};
        	if($@){
        		if($dry_run){
        			
        		}
        		&debug('CAUGHT',$@);
        	}
        	# We create a FH selector in order to swap between the STDOUT and
 	        # STDERR of the command
        	my $fh_selector = IO::Select->new();
        	my @fh_ready;
        	my $read_buffer; # Internal thread read buffer
        	my($outeof,$erreof) = (1,1); # This is to control the eof of our FH
	        # We connect STDOUT and STDERR to our FH selector
        	eval{$fh_selector->(*IPCSTDOUT,*IPCSTDERR)};
        	if($@){&debug('CAUGHT',$@)}
	        # Now start the big loop...
	        # We actually loop aroung the availability of data in our FH
        	while(@fh_ready = $fh_selector->can_read()){
            	if($read_ctrl == 0){
                	# The thread enter this place whenever someone outside ask it
	                # to stop
                	&debug('DIEDR');
	                # We need to kill pg_dump and wait for its corpse to rest in
	                # peace
    	            kill(2,$pgdpid);
        	        waitpid($pgdpid,0);
            	    $exitcode = $? >> 8;
                	&debug('EXITCMD',$exitcode);
                	threads->exit();
	            }
    	        foreach my $fh (@fh_ready){
        	        if(fileno($fh) == fileno(IPCSTDOUT)){
            	        # Our selector has found some data on STDOUT, let's process
                	    # this
                    	my $rbuf;
                    	$outeof = sysread($fh,$rbuf,$blk_size);
                    	$read_buffer .= $rbuf;
                    	if($outeof == 0){
                        	# We have reached STDOUT eof
                        	$fh_selector->remove($fh);
                        	&debug('STDOEOF');
                    	}
                	}
                	elsif(fileno($fh) == fileno(IPCSTDERR)){
                    	# Same stuff, different FH...
                    	my $rbuf;
                    	$erreof = sysread($fh,$rbuf,$blk_size);
                    	$errors .= $rbuf;
                    	if($erreof == 0){
	                        $fh_selector->remove($fh);
    	                    &debug('STDEEOF');
        	            }
            	    }else{
                	    &debug('ERRUKN');
                	}
            	}
            	# Let's try to get a lock on the shared buffer to purge the local
	            # read buffer
    	        if($buffer_semaphore->down_nb()){
        	        if($read_buffer){$buffer .= $read_buffer}
            	    $read_buffer = '';
                	$buffer_semaphore->up();
            	}
        	}
        	waitpid(sub{if($pgdpid){return($pgdpid)}},0);
        	$exitcode = $? >> 8;
        	&debug('EXITCMD',$exitcode);
        	&debug('CLOSEFH');
        	close(IPCSTDOUT);
        	close(IPCSTDERR);
        	if($errors){
	            &debug('STDERR',$errors);
        	}
        	&debug('LEAVEDR');
        }
    };

    # This is the thread to handle the actual data writting
    my $write_runner = sub {
        &debug('STARTWR');
        &debug('OPEN',$pg_dumpfile);
        eval {open(OUT,'>',$pg_dumpfile) or die($!)};
        if($@){
            # open has failed. No point going further.
            &debug('ERROUT',$pg_dumpfile,$@);
        }else{
        	while($write_ctrl or $buffer){
            	if($buffer){
                	$buffer_semaphore->down();
                	&debug('FLUSHBUF');
                	print OUT ($buffer);
                	$buffer = '';
                	$buffer_semaphore->up();
            	}
            	usleep($flush_delay);
        	}
        	&debug('EXITWR');
        	close(OUT);
        }
    };

    # Threads control block
    &debug('CALLWR');
    my $write_thr = threads->create($write_runner);
    &debug('CALLDR');
    my $dump_thr = threads->create($dump_runner);
    &debug('WAITDR');
    $dump_thr->join();
    &debug('ENDDR');
    &debug('STOPWR');
    $write_ctrl = 0;
    &debug('WAITWR');
    $write_thr->join();
    &debug('ENDWR');
    return($exitcode,)
}

sub bck_restore {}
sub notify {}
sub error {}
sub bck_rotate {}

sub debug {
    my $msg = shift;
    my @args = @_;
    my %messages = {
		'VERBOSE'  => [1,'Verbose mode activated'],
		'START'    => [2,'Starting run'],
		'END'      => [2,'End running'],
		'GETOPTS'  => [2,'Fetching getopts'],
		'ARGV'     => [2,'ARGV : %s'],
		'FULLDUMP' => [2,'Full dump'],
		'CAUGHT'   => [1,'Caught : %s'],
		'CALLWR'   => [2,'Calling write_runner thread'],
		'CALLDR'   => [2,'Calling dump_runner thread'],
		'WAITDR'   => [2,'Waiting for dump_runner to finish'],
		'ENDDR'    => [2,'dump_runner has finished'],
		'STOPWR'   => [2,'Asking write_runner to stop'],
		'WAITWR'   => [2,'Waiting for write_runner to finish'],
		'ENDWR'    => [2,'write_runner has finished'],
		'HELP'     => [2,'Calling for help'],
		'STARTDR'  => [2,'Starting dump thread %s'],
		'CMD'      => [2,'Starting command : %s'],
		'DRYRUN'   => [1,'Dryrun : %s'],
		'ERREXEC'  => [0,'Something wrong has happened while executing %s : %s : %s'],
		'BINMODE'  => [1,'Couldn\'t set binmode on program\'s STDOUT : %s'],
		'STDIN'    => [2,'Closing STDIN for safety as we don\'t need it'],
		'DIEDR'    => [0,'dump_runner as been asked to die'],
		'EXITCMD'  => [2,'pg_dump exited with exit code : %s'],
		'STDOEOF'  => [2,'STDOUT eof reached'],
		'STDEEOF'  => [2,'STDERR eof reached'],
		'ERRUKN'   => [0,'Something very wrong has happened'],
		'CLOSEFH'  => [2,'Closing all remaining file handles'],
		'STDERR'   => [1,'STDERR : %s'],
		'LEAVEDR'  => [2,'Leaving dump_runner'], 
		'STARTWR'  => [2,'Starting write_runner'],
		'OPEN'     => [2,'Opening %s'],
		'ERROUT'   => [0,'Something wrong has happened while trying to write output file %s : %s'],
		'FLUSHBUF' => [2,'Flushing buffer'],
		'EXITWR'   => [2,'Exiting write_runner']};
    my @loglevel = ('ERROR','WARNING','NOTIFY','DEBUG');
    if($error_level > $messages{$msg}->[0]){$error_level = $messages{$msg}->[0]} # lower global error level
    if(@args){
        for(my $index = 0; $index <= scalar(@args) - 1; $index++){
                $args[$index] =~ s/\n//;
        }
    }
    
    $msg = sprintf($messages{$msg}->[1],@args);
    if($DEBUGLEVEL){print STDERR ("[$$-".time."] $msg\n")}
    return;
}

sub HELP_MESSAGE {
    &debug('Calling for help');
    $Getopt::Std::STANDARD_HELP_VERSION = 1;
    my $fh = shift;
    if(! $fh){$fh = *STDERR}
    print $fh ("Usage : $0 [OPTIONS] [PATH]\n");
    print $fh ("Create an incremental or full dump of a postgres database,\n");
    print $fh ("handling dump rotation for long time backup purpose\n");
    print $fh ("  -h\t\tThis help\n");
    print $fh ("  -v\t\tVerbose mode\n");
    print $fh ("  -d\t\tDry run. Don't modify or write anything\n");
    print $fh ("  -i\t\tForce incremental backup\n");
    print $fh ("  -f\t\tForce full backup\n");
    print $fh ("  -D base\tDatabase to backup (else all)\n");
    print $fh ("  -k\t\tForce to keep previous backup, even if they are expired\n");
    print $fh ("  -F fmt\tBackup file format (plain,custom,tar)\n");
    print $fh ("  -x pgparams\tSpecify pg_dump specific arguments. See man pg_dump\n");
    print $fh ("Report bugs to <git\@nimporteou.net>\n");
}
