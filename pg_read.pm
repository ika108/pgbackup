#!/usr/bin/perl -w

package pg_read;

use strict;
use IPC::Open3;
use IO::Select;
use Time::HiRes qw(usleep);
use threads;
use threads::shared;
use Thread::Semaphore;

our $VERSION = 0.9;
my $pg_read_dumpcmd = '/usr/bin/pg_dump';
my $pg_read_blk_size = 1024;
my $pg_read_dmp_format = 'p';
my $pg_read_params = '';


sub new {
	my $class = shift;

	my $stateref :share = 'STOPPED';
	my $buffer :share = undef;
	my $read :share = undef;
	my $exitcode :share = undef;
	my $errors :share = undef;
	state($stateref,$buffer,$read,$exitcode,$errors); # We don't want the GC to destroy those, no no no...

	my $self = {};
	$self->{'STATE'} = \$stateref;
	$self->{'BUFFER'} = \$buffer;
	$self->{'READ'} = \$read;
	$self->{'EXITCODE'} = \$exitcode;
	$self->{'ERRORS'} = \$errors;
	$self->{'NOTIFIER'} = shift;
	$self->{'PARAMS'} = shift;
	$self->{'THREAD'} = undef;
	$self->{'COMMAND'} = undef;
	$self->{'CMDPID'} = undef;
	if($self->{'PARAMS'}->{'D'}){$self->{'DATABASE'} = $self->{'PARAMS'}->{'D'}}else{$self->{'DATABASE'} = 'all'}
	if($self->{'PARAMS'}->{'F'}){$self->{'FORMAT'} = $self->{'PARAMS'}->{'F'}}else{$self->{'FORMAT'} = $pg_read_dmp_format}
	if($self->{'PARAMS'}->{'x'}){$self->{'ARGS'} = $self->{'PARAMS'}->{'x'}}else{$self->{'ARGS'} = $pg_read_params}

	#### This our reading thread code. We will use it later 	
	$self->{'THREADCODE'} = sub {
		$self->{'NOTIFIER'}->notify('STARTDR',threads->tid());
		if(! $self->{'PARAMS'}->{'d'}){ # Not dryrun mode
			if($self->{'PARAMS'}->{'D'}){ # Database specified
				$self->{'COMMAND'} = "$pg_read_dumpcmd $self->{'DATABASE'} -F $self->{'FORMAT'} --create $self->{'ARGS'}";
			}else{
				# Nope. No database specified. Dumpall...
				$self->{'COMMAND'} = $pg_read_dumpcmd."all ".$self->{'ARGS'};
			}
		}else{
			if($self->{'PARAMS'}->{'D'}){
				$self->{'NOTIFIER'}->notify('DRYRUN',"$pg_read_dumpcmd $self->{'DATABASE'} -F $self->{'FORMAT'} --create $self->{'ARGS'}");
			}else{
				$self->{'NOTIFIER'}->notify('DRYRUN',$pg_read_dumpcmd."all ".$self->{'ARGS'});
			}
			# We fill our buffer with irrelenvent data just to test it
			$self->{'COMMAND'} = "/bin/dd if=/dev/zero bs=1 count=1048576 2>&1";
		}

		$self->{'NOTIFIER'}->notify('CMD',$self->{'COMMAND'});

		# We use open3 instead of open because we need to catch every FH
		eval { $self->{'CMDPID'} = open3(\*IPCSTDIN,\*IPCSTDOUT,\*IPCSTDERR,$self->{'COMMAND'}) };
		if($@){
			# open3 has failed, no point going further.
			my $stderr = join('/',<IPCSTDERR>);
			$self->{'NOTIFIER'}->notify('ERREXEC',$self->{'COMMAND'}, $@, $stderr);
			$self->{'NOTIFIER'}->notify('CLOSEFH');
			close(IPCSTDIN);
			close(IPCSTDOUT);
			close(IPCSTDERR);
		}else{
			# binmode might not be necessary, but as we process binary data (globs)
			# better safe than sorry
			eval{no warnings 'all' ; binmode(IPCSTDOUT)};
			if($@){
				if($self->{'PARAMS'}->{'d'}){
					$self->{'NOTIFIER'}->notify('CAUGHT',$@);
				}else{
					$self->{'NOTIFIER'}->notify('BINMODE',$@);
				}
			}

			$self->{'NOTIFIER'}->notify('STDIN');
			eval{close(IPCSTDIN)};
			if($@){
				if($self->{'PARAMS'}->{'d'}){
					$self->{'NOTIFIER'}->notify('CAUGHT',$@);
				}
			}

			# We create a FH selector in order to swap between the STDOUT and STDERR of the command
			my $fh_selector = IO::Select->new();
			my @fh_ready;
			my $read_buffer; # Internal thread read buffer
			my($outeof,$erreof) = (1,1); # This is to control the eof of our FH We connect STDOUT and STDERR to our FH selector

			eval{$fh_selector->add(\*IPCSTDOUT,\*IPCSTDERR)};
			if($@){$self->{'NOTIFIER'}->notify('CAUGHT',$@)}


			# Now start the big loop...
			# We actually loop around the availability of data in our FH
			while(@fh_ready = $fh_selector->can_read() ){

				# Suspended activty
				while(${$self->{'STATE'}} eq 'STOPPED'){
					sleep(1);
					$self->{'NOTIFIER'}->notify('WAITRUN');
				}

				# We have been asked to end it all
				if(${$self->{'STATE'}} eq 'END'){
					# The thread enter this place whenever someone outside ask it to stop
					$self->{'NOTIFIER'}->notify('DIEDR');
					# We need to kill pg_dump and wait for its corpse to rest in peace
					if($self->{'PARAMS'}->{'d'}){$self->{'EXITCODE'} = 0}
					else{
						$self->{'NOTIFIER'}->notify('NOTIFY',"Killing $self->{'CMDPID'}");
						kill(2,$self->{'CMDPID'});
						waitpid($self->{'CMDPID'},0);
						$self->{'EXITCODE'} = $?;
					}
					$self->{'NOTIFIER'}->notify('CLOSEFH');
					close(IPCSTDOUT);
					close(IPCSTDERR);
					$self->{'NOTIFIER'}->notify('EXITCMD',$self->{'EXITCODE'});
					if($self->{'EXITCODE'} > 0){$self->{'NOTIFIER'}->notify('UNXPEND')}
				}

				# No reason to stop
				else{
					foreach my $fh (@fh_ready){
						if(fileno($fh) == fileno(IPCSTDOUT)){
							# Our selector has found some data on STDOUT, let's process this
							my $rbuf;
							$outeof = sysread($fh,$rbuf,$pg_read_blk_size);
							$read_buffer .= $rbuf;
							if($outeof == 0){
								# We have reached STDOUT eof
								$fh_selector->remove($fh);
								$self->{'NOTIFIER'}->notify('STDOEOF');
							}
						}elsif(fileno($fh) == fileno(IPCSTDERR)){
							# Same stuff, different FH...
							my $rbuf;
							$erreof = sysread($fh,$rbuf,$pg_read_blk_size);
							$self->{'ERRORS'} .= $rbuf;
							if($erreof == 0){
								$fh_selector->remove($fh);
								$self->{'NOTIFIER'}->notify('STDEEOF');
							}
						}else{
							$self->{'NOTIFIER'}->notify('ERRUKN');
						}
					}

					# Let's try to get a lock on the shared buffer to purge the local read buffer
					if($read_buffer){
						lock(${$self->{'BUFFER'}});
						# $self->{'NOTIFIER'}->notify('LOCK');
						${$self->{'BUFFER'}} .= $read_buffer;
						$self->{'BUFFERSIZE'} = length(${$self->{'BUFFER'}});
						${$self->{'READ'}} += $self->{'BUFFERSIZE'};
						$read_buffer = '';
					}
				}
			}
		} # Nothing to read anymore

		if($self->{'PARAMS'}->{'d'}){$self->{'EXITCODE'} = 0}
		else{
			$self->{'NOTIFIER'}->notify('NOTIFY',"Waiting $self->{'CMDPID'}");
			waitpid($self->{'CMDPID'},0);
			$self->{'EXITCODE'} = $?;
		}
		$self->{'NOTIFIER'}->notify('EXITCMD',$self->{'EXITCODE'});
		if($self->{'EXITCODE'} > 0){$self->{'NOTIFIER'}->notify('UNXPEND')}
		$self->{'NOTIFIER'}->notify('CLOSEFH');
		close(IPCSTDOUT);
		close(IPCSTDERR);
		if($self->{'ERRORS'}){
			$self->{'NOTIFIER'}->notify('STDERR',$self->{'ERRORS'});
		}
		$self->{'NOTIFIER'}->notify('LEAVEDR');
	};
	#### End of thread definition
	$self->{'THREAD'} = threads->create($self->{'THREADCODE'});
	bless($self,$class);
	return($self);
}

sub start {
	my $self = shift;
	$self->{'NOTIFIER'}->notify('CALLWR');
	${$self->{'STATE'}} = 'RUN';
	return($self->{'THREAD'});
}

sub stop {
	my $self = shift;
	$self->{'NOTIFIER'}->notify('STOPWR');
	${$self->{'STATE'}} = 'STOPPED';
}

sub end {
	my $self = shift;
	$self->{'NOTIFIER'}->notify('WAITWR');
	${$self->{'STATE'}} = 'END';
	$self->{'THREAD'}->join();
}

sub state {
	my $self = shift;
	return(${$self->{'STATE'}});
}

sub buffersize {
	my $self = shift;
	return(length(${$self->{'BUFFER'}}));
}

sub buffer {
	my $self = shift;
	if($_){${$self->{'BUFFER'}} = shift}
	return(${$self->{'BUFFER'}});
}

sub read {
	my $self = shift;
	return(${$self->{'READ'}});
}

sub exitcode {
	my $self = shift;
	return(${$self->{'EXITCODE'}});
}

sub errors {
	my $self = shift;
	return(${$self->{'ERRORS'}});
}

1;