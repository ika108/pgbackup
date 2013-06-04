#!/usr/bin/perl -w

package pg_notify;

use strict;
use Time::HiRes qw(usleep);

our $VERSION = 0.9;
my $logfile = '/var/log/pg_backup.log';
my @pg_notify_rcpt = qw(git@nimporteou.net);
my @verboselevel = ('ERROR','WARNING','NOTIFY','DEBUG');
my %errorlevel = ('DEBUG' => 0, 'NOTIFY' => 1, 'WARNING' => 2, 'ERROR' => 3);
my $DEBUGLEVEL = 'DEBUG';

my %messages = (
	'ERREXEC'  => ['ERROR',		'Something wrong has happened while executing %s : %s : %s'],
	'DIEDR'    => ['ERROR',		'dump_runner as been asked to die'],
	'ERRUKN'   => ['ERROR',		'Something very wrong has happened'],
	'INT'	   => ['ERROR',		'Signal INT has been called'],
	'UNXPEND'  => ['ERROR',		'Unexpected end of command'],
	'ERROUT'   => ['ERROR',		'Something wrong has happened while trying to write output file %s : %s'],
	'ERROR'	   => ['ERROR',		'An error occured during backup execution. please check the logs and try again. (no rotation)'],
	'CAUGHT'   => ['WARNING',	'Caught : %s'],
	'BINMODE'  => ['WARNING',	'Couldn\'t set binmode on program\'s STDOUT : %s'],
	'STDERR'   => ['WARNING',	'STDERR : %s'],
	'WARNING'  => ['WARNING',	'Some warning has occured. Please check the logs and try again. (no rotation)'],
	'START'    => ['NOTIFY',	'Starting run'],
	'END'      => ['NOTIFY',	'End running'],
	'FULLDUMP' => ['NOTIFY',	'Full dump'],
	'CMD'      => ['NOTIFY',	'Starting command : %s'],								
	'LOGLVL'   => ['NOTIFY',	'Final errror level : %s'],
	'ROTATE'   => ['NOTIFY',	'Starting dump archives rotation'],
	'DELETE'   => ['NOTIFY',	'Deleting : %s'],
	'NOTIFY'   => ['NOTIFY',	'Notify : %s']
	'VERBOSE'  => ['DEBUG',		'Verbose mode activated'],
	'GETOPTS'  => ['DEBUG',		'Fetching getopts'],
	'ARGV'     => ['DEBUG',		'ARGV : %s'],
	'CALLWR'   => ['DEBUG',		'Calling write_runner thread'],
	'CALLDR'   => ['DEBUG',		'Calling dump_runner thread'],
	'WAITDR'   => ['DEBUG',		'Waiting for dump_runner to finish'],
	'ENDDR'    => ['DEBUG',		'dump_runner has finished'],
	'STOPWR'   => ['DEBUG',		'Asking write_runner to stop'],
	'WAITWR'   => ['DEBUG',		'Waiting for write_runner to finish'],
	'ENDWR'    => ['DEBUG',		'write_runner has finished'],
	'HELP'     => ['DEBUG',		'Calling for help'],
	'STARTDR'  => ['DEBUG',		'Starting dump thread %s'],
	'DRYRUN'   => ['DEBUG',		'Dryrun : %s'],
	'STDIN'    => ['DEBUG',		'Closing STDIN for safety as we don\'t need it'],
	'EXITCMD'  => ['DEBUG',		'pg_dump exited with exit code : %d'],
	'STDOEOF'  => ['DEBUG',		'STDOUT eof reached'],
	'STDEEOF'  => ['DEBUG',		'STDERR eof reached'],
	'CLOSEFH'  => ['DEBUG',		'Closing all remaining file handles'],
	'LEAVEDR'  => ['DEBUG',		'Leaving dump_runner'],
	'STARTWR'  => ['DEBUG',		'Starting write_runner'],
	'OPEN'     => ['DEBUG',		'Opening %s'],
	'LOCK'	   => ['DEBUG',		'Aquiring lock'],
	'FLUSHBUF' => ['DEBUG',		'Flushing buffer : %s Bytes'],
	'EXITWR'   => ['DEBUG',		'Exiting write_runner'],
	'HELP'     => ['DEBUG',		'Calling for help'],
	'DELAY'	   => ['DEBUG',		'Postponing %s deletation until %s']);



sub new {
	my $class = shift;
	# We don't want to continue further if we can't write to our logfile.
	if(!-w $logfile){die("Can't open $logfile for writting exiting now...")}
	my $self = {};
	$self->{'PARAMS'} = shift;
	$self->{'READEXIT'} = undef;
	$self->{'WRITEEXIT'} = undef;
	$self->{'READERR'} = undef;
	$self->{'WRITEERR'} = undef;
	$self->{'ERRORLEVEL'} = 0;
	$self->{'VERBOSITY'} = $DEBUGLEVEL;
	$self->{'QUIETREPORT'} = undef;
	bless($self,$class);
	return($self);
}

sub notify {
	my $self = shift;
	my $msg = shift;
    my @args = @_;
    my $textmsg;
    if(!$messages{$msg}){print("BUG : don't know how to handle this message : $msg\n")}
    if($self->{'ERRORLEVEL'} < $errorleve{$messages{$msg}->[0]}){
    	$self->{'ERRORLEVEL'} = $errorleve{$messages{$msg}->[0]}; # raise global error level
    }
    if(@args and ( $args[0] or $args[0] eq 0 )){
        for(my $index = 0; $index <= scalar(@args) - 1; $index++){
            if($args[$index]){
            	$args[$index] =~ s/\n//;  # Discard any carriage return
            }
        }
        $textmsg = sprintf($messages{$msg}->[1],@args); # Fetch our message content (With extra printf args)
    }
    else{$textmsg = $messages{$msg}->[1]} # Same but with no extra args
    if($self->{'PARAMS'->{'q'}}){
    	# Quiet mode. Don't output anything now.
    	$self->{'QUIETREPORT'} .= sprintf("[%i-%.6f] %s %s\n",$$,Time::HiRes::time(),$messages{$msg}->[0],$textmsg);
    }
    else{
    	if($errorlevel{$messages{$msg}->[0]} >= $errorlevel{'WARNING'}){
    		# Warning or error, we should output some info to STDERR
    		printf STDERR ("[%i-%.6f] %s %s\n",$$,Time::HiRes::time(),$messages{$msg}->[0],$textmsg);
    		eval {open(LOGFILE,'>',$logfile) or die("Can't open $logfile for writting : $! but we did check earlier !?");}
    		if($@){printf STDERR ("[%i-%.6f] %s %s\n",$$,Time::HiRes::time(),'ERROR',"$@")} # We should be able to write to this file, but couldn't. to late to quit now
    		else{
	    		printf LOGFILE ("[%i-%.6f] %s %s\n",$$,Time::HiRes::time(),$messages{$msg}->[0],$textmsg);
    			close(LOGFILE);
    		}
    	}
	    elsif($errorlevel{$messages{$msg}->[0]} == $errorlevel{'NOTIFY'}){
    		if($self->{'VERBOSITY'} <= $errorlevel{'NOTIFY'}) {
    			# We shouldn't output anything here, unless the verbosity level is equal or above NOTIFY
    			printf STDERR ("[%i-%.6f] %s %s\n",$$,Time::HiRes::time(),$messages{$msg}->[0],$textmsg);
    		}
    	}
    	elsif($errorlevel{$messages{$msg}->[0]} == $errorlevel{'DEBUG'}){
	    	if($self->{'VERBOSITY'} <= $errorlevel{'DEBUG'}) {
    			# We shouldn't output anything here, unless the verbosity level is equal to DEBUG
    			printf STDERR ("[%i-%.6f] %s %s\n",$$,Time::HiRes::time(),$messages{$msg}->[0],$textmsg);
    		}
    	}
    }
    return();
}

sub report {
	my $self = shift;
	
}

1;