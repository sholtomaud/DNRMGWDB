#!perl -T
use 5.006;
use strict;
use warnings FATAL => 'all';
use Test::More;

# Ensure a recent version of Test::Pod
eval "use Test::Pod";
plan skip_all => "Test::Pod Error " if $@;

all_pod_files_ok();
