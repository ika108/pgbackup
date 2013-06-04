#!/usr/bin/perl -w

package pg_write;

use strict;
use File::Find;
use threads;
use threads::shared;
use Thread::Semaphore;

our $VERSION = 0.9;

my $pg_write_dumpdir = '/dump';
my $pg_write_flush_delay = 100000; # In microsecond = 0.1 second
my $pg_write_retention = 4; # Retain 8 sucessfull instances
my $pg_write_mindelay = 172800; # Don't delete file not older than 48 hours


sub new {
	my $class = shift;
	my $params_ref = shift;
	my $self = {};
	if($params_ref->{'D'}){
		$self->{'DUMPFILE'} = "$pg_write_dumpdir/db-".$params_ref->{'D'}.'-'.time;
		$self->{'DB'} = $params_ref->{'D'};
	}else{
		$self->{'DUMPFILE'} = "$pg_write_dumpdir/db-all-".time;
		$self->{'DB'} = 'all';
	}
	
	bless($self,$class);
	return($self);
}

    
1;