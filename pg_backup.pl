#! /usr/bin/perl -w

# Load additional library
use strict;
use Getopt::Std;

our $VERSION = 0.9;

my @notify_rcpt = qw(git@nimporteou.net);
my $panic;

$SIG{INT} = sub {
    &debug('INT');
    $panic = 1;
};

&run();

sub run {
    if(grep(/-v/,@ARGV)){
        $DEBUGLEVEL = 3;
        &debug('VERBOSE');
    }
    &debug('START');
    my $params = &get_params();
    if($params->{'h'}){&HELP_MESSAGE; &debug('END'); exit(0);}
    if($params->{'d'}){$dry_run = 1}
    if($params->{'i'}){}
    if($params->{'f'}){$format = $params->{'F'}}
    if($params->{'k'}){$keep = 1}
    if($params->{'x'}){$pg_params = $params->{'x'}}
    my $backup_state = &bck_dump($params);
    &debug('LOGLVL',$loglevel[$error_level]);
    if($error_level == 1){&debug('WARNING'); rename($pg_dumpfile,"$pg_dumpfile-WARNING")} 
    elsif($error_level == 0){&debug('ERROR'); rename($pg_dumpfile,"$pg_dumpfile-ERROR")}
    else{&bck_rotate()}
    &debug('END');
    exit(0);
}

sub get_params {
    &debug('GETOPTS');
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
	&debug('ARGV',$argstring);

if($opts{'ARGV'} and ! (-w $opts{'ARGV'}) ){
    &error
}
return(\%opts);
}

sub bck_dump {
    &debug('FULLDUMP');
    my $params = shift;
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
	my $pgdcmd;
	if(!$dry_run){
	    if($params->{'D'}){
	        $pgdcmd = "$pg_dump $pg_db -F $format --create $pg_params";
	    }else{
                $pgdcmd = "$pg_dump $pg_params";
	    }
	}else{
	    if($params->{'D'}){&debug('DRYRUN',"$pg_dump $pg_db -F $format --create $pg_params")}
	    else{&debug('DRYRUN',"$pg_dump $pg_params")}
	    $pgdcmd = "/bin/dd if=/dev/zero bs=1 count=1048576 2>&1";
	}
        &debug('CMD',$pgdcmd);
        my $pgdpid;
        # We use open3 instead of open because we need to catch every FH
        eval { $pgdpid = open3(\*IPCSTDIN,\*IPCSTDOUT,\*IPCSTDERR,$pgdcmd) };
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
            eval{$fh_selector->add(\*IPCSTDOUT,\*IPCSTDERR)};
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
		    if($dry_run){$exitcode = 0}
		    else{
			&debug('NOTIFY',"Killing $pgdpid");
                        kill(2,$pgdpid);
                        waitpid($pgdpid,0);
                        $exitcode = $?;
		    }
                    &debug('EXITCMD',$exitcode);
		    if($exitcode > 0){&debug('UNXPEND')}
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
                $buffer_semaphore->down();
	        # &debug('LOCK');
		if($read_buffer){$buffer .= $read_buffer}
		$read_buffer = '';
		$buffer_semaphore->up();
		if($panic){$read_ctrl = 0}
            }
	    if($dry_run){$exitcode = 0}
	    else{
		&debug('NOTIFY',"Waiting $pgdpid");
	        waitpid($pgdpid,0);
                $exitcode = $?;
	    }
            &debug('EXITCMD',$exitcode);
	    if($exitcode > 0){&debug('UNXPEND')}
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
        if(! $dry_run){eval {open(OUT,'>',$pg_dumpfile) or die($!)}}
	else{
	    &debug('DRYRUN', "Writing to $pg_dumpfile");
	    open(OUT,'>','/dev/null') or die($!);
	}
        if($@){
            # open has failed. No point going further.
            &debug('ERROUT',$pg_dumpfile,$@);
        }else{
            while($write_ctrl or $buffer){
                if($buffer){
                    $buffer_semaphore->down();
                    &debug('FLUSHBUF',length($buffer));
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
    return($errors);
}

sub bck_restore {}

sub bck_rotate {
	&debug('ROTATE');
	$pg_dumpfile =~ m/^(.*-)\d+$/;
	my $fileprefix = $1;
	my %files;
	no warnings 'File::Find';
	find sub { if($File::Find::name =~ m/$fileprefix(\d+)/){$files{$1} = $File::Find::name } } , $pg_dumpdir;
	my @timestamps = sort {$b cmp $a} (keys(%files));
	splice(@timestamps,0,$retention);
	foreach my $timestamp (@timestamps){
		if( ( ( time - $timestamp ) > $mindelay ) or $keep ){
			&debug('DELETE',$fileprefix.$timestamp);
			if(!$dry_run){
				unlink($fileprefix.$timestamp);
			}	
		}else{
			my @abbr = qw( Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec ); 
			my @abbr1 = qw( Mon Tue Wed Thu Fri Sat Sun);
			my @date = localtime($timestamp + $mindelay);
			&debug('DELAY',$fileprefix.$timestamp, "$abbr1[$date[6]] $abbr[$date[4]] $date[3] ".sprintf($date[2]).":".sprintf($date[1]).":".$date[0]." ".($date[5] + 1900));
		}
	}
	return;
}

sub debug {






    return;
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

