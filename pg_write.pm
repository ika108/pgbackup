#!/usr/bin/perl -w

package pg_write;

use strict;
use File::Find;
use threads;
use threads::shared;
use Thread::Semaphore;
use File::ReadBackwards;

our $VERSION = 0.9;

my $pg_write_dumpdir = '/dump';
my $pg_write_flush_delay = 100000; # In microsecond = 0.1 second
my $pg_write_retention = 4; # Retain 8 sucessfull instances
my $pg_write_mindelay = 172800; # Don't delete file not older than 48 hours
my $pg_dumpdb_pattern = '-- PostgreSQL database cluster dump complete';
my $pg_dumpall_pattern = '-- PostgreSQL database dump complete';


sub new {
	my $class = shift;

	my $stateref :share = 'STOPPED';
	my $buffer :share = undef;
	my $written :share = undef;
	state($stateref,$buffer,$written); # We don't want the GC to destroy those, no no no...

	my $self = {};
	$self->{'STATE'} = \$stateref;
	$self->{'BUFFER'} = \$buffer;
	$self->{'WRITTEN'} = \$written;
	$self->{'PARAMS'} = shift;
	$self->{'NOTIFIER'} = shift;
	$self->{'THREAD'} = undef;
	$self->{'ENDREACH'} = undef;
	if($self->{'PARAMS'}->{'D'}){
		$self->{'DUMPFILE'} = "$pg_write_dumpdir/db-".$self->{'PARAMS'}->{'D'}.'-'.time;
		$self->{'DB'} = $self->{'PARAMS'}->{'D'};
	}else{
		$self->{'DUMPFILE'} = "$pg_write_dumpdir/db-all-".time;
		$self->{'DB'} = 'all';
	}
	
	#### This our writting thread code. We will use it later 
	$self->{'THREADCODE'} = sub {
		$self->{'NOTIFIER'}->notify('STARTWR',threads->tid());
		$self->{'NOTIFIER'}->notify('OPEN',$self->{'DUMPFILE'});
		if(! $self->{'PARAMS'}->{'d'}){eval {open(OUT,'>',$self->{'DUMPFILE'}) or die($!)}}
		else{
			# Dryrun detected. don't write anything, just pretend
			$self->{'NOTIFIER'}->notify('DRYRUN', "Writing to $self->{'DUMPFILE'}");
			open(OUT,'>','/dev/null') or die($!);
		}
		if($@){
			# open has failed. No point going further.
			$self->{'NOTIFIER'}->notify('ERROUT',$self->{'DUMPFILE'},$@);
		}else{
			while(${$self->{'STATE'}} ne 'END' or ${$self->{'BUFFER'}}){
				while(${$self->{'STATE'}} eq 'STOPPED'){
					sleep(1);
					$self->{'NOTIFIER'}->notify('WAITRUN');
				}
				if(${$self->{'BUFFER'}}){
					lock(${$self->{'BUFFER'}});
					$self->{'NOTIFIER'}->notify('FLUSHBUF',length(${$self->{'BUFFER'}}));
					${$self->{'WRITTEN'}} += length(${$self->{'BUFFER'}});
					if(! syswrite(OUT,${$self->{'BUFFER'}})){
						$self->{'NOTIFIER'}->notify('ERROUT',$!);
						close(OUT);
						last;
					}
					print OUT (${$self->{'BUFFER'}});
					${$self->{'BUFFER'}} = '';
				}
				usleep($pg_write_flush_delay);
			}
			$self->{'NOTIFIER'}->notify('EXITWR');
			close(OUT);
		}
		${$self->{'STATE'}} = 'END';
	};
	#### End of thread definition
	$self->{'THREAD'} = threads->create($self->{'THREADCODE'});
	bless($self,$class);
	return($self);
}

sub rotate {
	my $self = shift;
	$self->{'NOTIFIER'}->notify('ROTATE');
	$self->{'DUMPFILE'} =~ m/^(.*-)\d+$/;
	my $fileprefix = $1;
	my %files;
	no warnings 'File::Find';
	find sub { if($File::Find::name =~ m/$fileprefix(\d+)/){$files{$1} = $File::Find::name } } , $pg_write_dumpdir;
	my @timestamps = sort {$b cmp $a} (keys(%files));
	splice(@timestamps,0,$pg_write_retention);
	foreach my $timestamp (@timestamps){
		if( ( ( time - $timestamp ) > $pg_write_mindelay ) or $self->{'PARAMS'}->{'k'} ){
			$self->{'NOTIFIER'}->notify('DELETE',$fileprefix.$timestamp);
			if(!$self->{'PARAMS'}->{'d'}){
				unlink($fileprefix.$timestamp);
			}	
		}else{
			my @abbr = qw( Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec ); 
			my @abbr1 = qw( Mon Tue Wed Thu Fri Sat Sun);
			my @date = localtime($timestamp + $pg_write_mindelay);
			$self->{'NOTIFIER'}->notify('DELAY',$fileprefix.$timestamp, "$abbr1[$date[6]] $abbr[$date[4]] $date[3] ".sprintf($date[2]).":".sprintf($date[1]).":".$date[0]." ".($date[5] + 1900));
		}
	}
	return;
}

sub dumpfile {
	my $self = shift;
	if($_){$self->{'DUMPFILE'} = shift}
	return($self->{'DUMPFILE'});
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

sub written {
	my $self = shift;
	return(${$self->{'WRITTEN'}});
}

sub endreach {
	my $self = shift;
	my $linecount;
	my $pattern;
	if($self->{'PARAMS'}->{'D'}){
		$pattern = $pg_dumpdb_pattern;
	}else{
		$pattern = $pg_dumpall_pattern;
	}
	my $bw = File::ReadBackwards->new($self->{'DUMPFILE'});
	while($linecount < 10 and ! $bw->eof()){
		if($bw->readline() =~ m/$pattern/){
			$self->{'NOTIFIER'}->notify('ENDREACH');
			$bw->close();
			return(1);
		}
	}
	$self->{'NOTIFIER'}->notify('CORRUPT');
	return(0);
}

1;