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
	my $params_ref = shift;
	my $self = {};
	bless($self,$class);
	return($self);
}

1;