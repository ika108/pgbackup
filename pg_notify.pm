#!/usr/bin/perl -w

package pg_notify;

use strict;
use Time::HiRes qw(usleep);
use Mail::Sendmail;
use Sys::Hostname;

our $VERSION = 0.9;

my $logfile = '/var/log/pg_backup.log';
my @pg_notify_rcpt = qw(git@nimporteou.net);
my $DEBUGLEVEL = 'NORMAL';

my @verboselevel = ('ERROR','WARNING','NOTIFY','DEBUG');
my %errorlevel = ('DEBUG' => 0, 'NOTIFY' => 1, 'WARNING' => 2, 'ERROR' => 3);
my @monthes = qw( Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec ); 
my @days = qw( Mon Tue Wed Thu Fri Sat Sun);


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
	'NOTIFY'   => ['NOTIFY',	'Notify : %s'],
	'VERBOSE'  => ['DEBUG',		'Verbose mode activated'],
	'GETOPTS'  => ['DEBUG',		'Fetching getopts'],
	'ARGV'     => ['DEBUG',		'ARGV : %s'],
	'CALLWR'   => ['DEBUG',		'Calling write_runner thread'],
	'CALLDR'   => ['DEBUG',		'Calling dump_runner thread'],
	'WAITDR'   => ['DEBUG',		'Waiting for dump_runner to finish'],
	'ENDDR'    => ['DEBUG',		'dump_runner has finished'],
	'STOPWR'   => ['DEBUG',		'Asking write_runner to stop'],
	'WAITWR'   => ['DEBUG',		'Waiting for write_runner to finish'],
	'WAITRUN'  => ['DEBUG',		'Waiting RUN signal to start writing'],
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
	'STARTWR'  => ['DEBUG',		'Starting write thread %s'],
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
	$self->{'PARAMS'} = undef;
	$self->{'READEXIT'} = undef;
	$self->{'READERR'} = 'None';
	$self->{'READTIME'} = 0;
	$self->{'WRITETIME'} = 0;
	$self->{'ROTATED'} = 'No';
	$self->{'ERRORLEVEL'} = 0;
	$self->{'VERBOSITY'} = $DEBUGLEVEL;
	$self->{'QUIETREPORT'} = undef;
	bless($self,$class);
	return($self);
}

# => Scalar : message code (See %pg_notify::messages before)
# => Array  : a list of args. depending of the message code specify
# <= Scalar : the newly set notify errorlevel. Remember, errorlevel can only raise...
sub notify {
	my $self = shift;
	my $msg = shift;
    my @args = @_;
    my $textmsg;
    if(!$messages{$msg}){print("BUG : don't know how to handle this message : $msg\n")}
    if($self->{'ERRORLEVEL'} < $errorlevel{$messages{$msg}->[0]}){
    	$self->{'ERRORLEVEL'} = $errorlevel{$messages{$msg}->[0]}; # raise global error level
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
    if($self->{'PARAMS'}->{'q'}){
    	# Quiet mode. Don't output anything now.
    	$self->{'QUIETREPORT'} .= sprintf("[%i-%.6f] %s %s\n",$$,Time::HiRes::time(),$messages{$msg}->[0],$textmsg);
    }
    else{
    	if($errorlevel{$messages{$msg}->[0]} >= $errorlevel{'WARNING'}){
    		# Warning or error, we should output some info to STDERR
    		printf STDERR ("[%i-%.6f] %s %s\n",$$,Time::HiRes::time(),$messages{$msg}->[0],$textmsg);
    		eval {open(LOGFILE,'>',$logfile) or die("Can't open $logfile for writting : $! but we did check earlier !?")};
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
    return($self->{'ERRORLEVEL'});
}

sub report {
	my $self = shift;
	my @date = time;
	my $report = "$0 execution report ($days[$date[6]] $monthes[$date[4]] $date[3] ".sprintf($date[2]).":".sprintf($date[1]).":".$date[0]." ".($date[5] + 1900).")\n";
	$report .= 'ARGV :                    ';
	foreach my $arg (keys(%{$self->{'PARAMS'}})){
    	$report .= "$arg => $self->{'PARAMS'}->{$arg}";
	}
	$report .= "\n";
	$report .= "Dump execution time :     $self->{'READTIME'}\n";
	$report .= "Writting execution time : $self->{'WRITETIME'}\n";
	$report .= "Dump size :               $self->{'DUMPSIZE'}\n";
	$report .= "pg_dump exit code :       $self->{'READEXIT'}\n";
	$report .= "Dump rotation :           $self->{'ROTATED'}\n";
	$report .= "Verbosity :               $self->{'VERBOSITY'}\n";
	$report .= "Final error level :       $self->{'ERRORLEVEL'}\n";
	$report .= "Errors from pg_dump :     \n$self->{'READERR'}\n";
	$report .= "Execution logs :          \n$self->{'QUIETREPORT'}\n";
	$report .= "End of report\n";
	
	foreach my $rcpt (@pg_notify_rcpt){
		my %mail = ('To'		=> $rcpt,
					'Subject'	=> 'pg_backup execution on '.hostname().": $self->{'ERRORLEVEL'}",
					'Message'	=> $report);
		sendmail(%mail) || print STDERR ("Error sending report by sendmail : ".$Mail::Sendmail::error."\nUsing cron : \n$report");
	}
}

# Simple set accessors

sub readtime {
	my $self = shift;
	$self->{'READTIME'} = shift;
}

sub writetime {
	my $self = shift;
	$self->{'WRITETIME'} = shift;
}

sub dumpsize {
	my $self = shift;
	$self->{'DUMPSIZE'} = shift;
}

sub readexit {
	my $self = shift;
	$self->{'READEXIT'} = shift;
}

sub rotated {
	my $self = shift;
	$self->{'ROTATED'} = shift;
}

sub verbosity {
	my $self = shift;
	$self->{'VERBOSITY'} = shift;
}

sub params {
	my $self = shift;
	$self->{'PARAMS'} = shift;
}

sub errorlevel {
	my $self = shift;
	return($self->{'ERRORLEVEL'});
}

1;