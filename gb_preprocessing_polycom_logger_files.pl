#!/usr/bin/perl

use strict;
use File::Find::Rule;

use Data::Dumper;

my $rule =  File::Find::Rule->new;

#preprocessing logger files...
$rule->file
     ->not( $rule->new->name( qr/^.+gb_preprocessed.+$/ ) );
$rule->name( qr/^Log.+\.log$/ );
my @files_to_preprocess;
my $dir_to_preprocess = $ARGV[0];
if( defined($dir_to_preprocess) ){
    @files_to_preprocess = $rule->in( "$dir_to_preprocess" );
}
else{
    @files_to_preprocess = $rule->in( "." );
}
print "START PREPROCESSING LOGGER FILES\n";
foreach my $file_to_preprocess(@files_to_preprocess){
    my $preprocessed_file = $file_to_preprocess;
    my ($log_sequence_no, $log_date, $log_time) = ( $file_to_preprocess =~ /.*Log(.+)_(.+)_(.+)\.log/ );
    my ($ticket_no) = ( $file_to_preprocess =~ /1-(\d+)\// );
    if(!$ticket_no){
        print "Preprocessing avoided for file=$file_to_preprocess as ticket number not available\n";
        next;
    }
    $log_date =~ s/(\d+)-(\d+)-(\d+)/$3-$2-$1/g;
    $log_time =~ s/-/\:/g;
    $preprocessed_file =~ s/(.+)(\..+)$/${1}_gb_preprocessed${2}/;
    print time.":Preprocessing file=$file_to_preprocess\n";
    open(FHR, $file_to_preprocess) || die "read file handle error=$@, $!\n";
    open(FHW, ">$preprocessed_file") || die "write file handle error=$@, $!\n";
    print FHW "LOGGER::${log_sequence_no}::${log_date} ${log_time}\n";
    my $d_block;
    foreach my $line(<FHR>){
        #tidy-fying the line...
        chomp($line);
        $line =~ s/^\s+//g;
        $line =~ s/\s+$//g;
        $line =~ s/\s+/ /g;
        #d-blocking lines...
        if(!$d_block && $line =~ /^D\:/){
            #the first d-block, the space before the pipe is required as it makes sure we have a proper delimiter
            $d_block = $line." |";
        }
        elsif($d_block && $line =~ /^D\:/){
            #change pipe-groups to single pipe and remove trailing pipes for each d-block line...
            $d_block =~ s/\|+/\|/g;
            $d_block =~ s/\|+$//g;
            #if the next d-block has started then print the current d-block to the output file
            print FHW "$d_block\n";
            #re-initialise the next d-block, the space before the pipe is required as it makes sure we have a proper delimiter
            $d_block = $line." |";
        }
        else{
            #if the line is not the start of a d-block then its the remainder part of a d-block
            $d_block .= $line."|";
        }
    }
    #print the last d-block to the output file :-)...
    if($d_block){
        #change pipe-groups to single pipe and remove trailing pipes for each d-block line...
        $d_block =~ s/\|+/\|/g;
        $d_block =~ s/\|+$//g;
        print FHW "$d_block\n";
    }
    print time.":Preprocessed file=$preprocessed_file\n";
    close(FHW);
    close(FHR);
    #exit;
}

#preprocessing Faults.txt files...
print "\n\nSTART PREPROCESSING FAULTS FILES\n";
my $rule2 =  File::Find::Rule->new;
$rule2->file;
$rule2->name( qr/^Faults\.txt$/ );
if( defined($dir_to_preprocess) ){
    @files_to_preprocess = $rule2->in( "$dir_to_preprocess" );
}
else{
    @files_to_preprocess = $rule2->in( "." );
}

foreach my $file_to_preprocess(@files_to_preprocess){
    my $preprocessed_file = $file_to_preprocess;
    my ($ticket_no) = ( $file_to_preprocess =~ /1-(\d+)\// );
    if(!$ticket_no){
        print "Preprocessing avoided for file=$file_to_preprocess as ticket number not available\n";
        next;
    }
    #print qq{$file_to_preprocess, $ticket_no\n};
    #next;
    $preprocessed_file =~ s/(.+)(\..+)$/${1}_gb_preprocessed${2}/;
    print time.":Preprocessing file=$file_to_preprocess\n";
    open(FHR, $file_to_preprocess) || die "read file handle error=$@, $!\n";
    open(FHW, ">$preprocessed_file") || die "write file handle error=$@, $!\n";
    print FHW "FAULT_TICKET::${ticket_no}\n";
    foreach my $line(<FHR>){
        print FHW $line;
    }
    print time.":Preprocessed file=$preprocessed_file\n";
    close(FHW);
    close(FHR);
    #exit;
}

#preprocessing CDR files...
print "\n\nSTART PREPROCESSING CDR FILES\n";
my $rule3 =  File::Find::Rule->new;
$rule3->file;
$rule3->file
     ->not( $rule3->new->name( qr/^.+gb_preprocessed.+$/ ) );
$rule3->name( qr/^.+\_.+\.txt$/ );
if( defined($dir_to_preprocess) ){
    @files_to_preprocess = $rule3->in( "$dir_to_preprocess" );
}
else{
    @files_to_preprocess = $rule3->in( "." );
}

foreach my $file_to_preprocess(@files_to_preprocess){
    my $preprocessed_file = $file_to_preprocess;
    my ($ticket_no) = ( $file_to_preprocess =~ /1-(\d+)\// );
    if( !defined($ticket_no) ){
        print "Preprocessing avoided for file=$file_to_preprocess as ticket number not available\n";
        next;
    }
    my ($bridge_name, $cdr_file_name) = ( $file_to_preprocess =~ /^.+\/(.+)\_(.+)\.txt$/ );
    if( !defined($bridge_name) || !defined($cdr_file_name) ){
        print "Preprocessing avoided for file=$file_to_preprocess as bridge name or cdr file name is not available\n";
        next;
    }
    #print qq{$file_to_preprocess, $ticket_no\n};
    #next;
    $preprocessed_file =~ s/(.+)(\..+)$/${1}_gb_preprocessed${2}/;
    print time.":Preprocessing file=$file_to_preprocess\n";
    open(FHR, $file_to_preprocess) || die "read file handle error=$@, $!\n";
    open(FHW, ">$preprocessed_file") || die "write file handle error=$@, $!\n";
    #print FHW "CDR_TICKET::${ticket_no}::${bridge_name}::${cdr_file_name}\n";
    my $cdr_block = "CDR_TICKET::${bridge_name}::${cdr_file_name}\n";
    my ($cdr_parent_event_block_number, $participant_id, $cdr_parent_event_date);
    my $cdr_block_number = 0;
    foreach my $line(<FHR>){
        #tidy-fying the line...
        chomp($line);
        $line =~ s/^\s+//g;
        $line =~ s/\s+$//g;
        $line =~ s/\s+/ /g;
        #cdr building blocks for parsing...
        if( $cdr_block =~ /^CDR_TICKET::.+\n\n+$/s && $line =~ /^[A-Z0-9_'s\s]+$/ && $line !~ /CONTINUE|PARTICIPANT INFO/){
            #entered the first cdr event after the conference initial details...also checking if event needs to be considered like a parent...
            $cdr_block =~ s/\n+$//s;
            $cdr_block =~ s/\n+/\n/gs;
            print FHW "$cdr_block\n--END_CDR_DETAILS--\n";
            $cdr_block_number++;
            $cdr_parent_event_block_number = $cdr_block_number;
            $cdr_block = "--BEGIN CDR BLOCK--".$cdr_block_number."|".$line."|${cdr_parent_event_block_number}"."\n";
        }
        elsif($cdr_block !~ /^CDR_TICKET::/s && $cdr_block =~ /\n\n+$/s && $line =~ /^[A-Z0-9_'s\s]+$/ && $line !~ /CONTINUE|PARTICIPANT INFO/){
            #entered a parent CDR event...parent CDR events dont have the following strings...CONTINUE, PARTICIPANT INFO...
            #also all events have two new-lines before it starts...
            #also store participant_id in case of participant events...
            if(!defined($participant_id)){
                #this will happen only if the previous parent doesnt have children and is a participant event...
                ($participant_id) = $cdr_block =~ /Participant ID:\s*(\d+)/;
            }
            my ($event_date) = $cdr_block =~ /--BEGIN CDR BLOCK--.+?\n(.+[A|P]M)\n/;
            if(!defined($event_date)){
                $event_date = $cdr_parent_event_date;
                $cdr_block =~ s/(--BEGIN CDR BLOCK--.+?)\n/$1\|${event_date}\n/g;
            }
            else{
                $cdr_block =~ s/(--BEGIN CDR BLOCK--.+?)\n(.+[A|P]M)\n/$1\|$2\n/g;
            }
            if(!defined($participant_id)){
                $cdr_block =~ s/(--BEGIN CDR BLOCK--.+?)\n/$1\|\n/g;
            }
            else{
                $cdr_block =~ s/(--BEGIN CDR BLOCK--.+?)\n/$1\|${participant_id}\n/g;
            }
            $cdr_block =~ s/\n+$//s;
            $cdr_block =~ s/\n+/\n/gs;
            print FHW "$cdr_block\n"."--END CDR BLOCK--\n";
            #re-initialise the next cdr-block parent...
            $cdr_block_number++;
            $cdr_parent_event_block_number = $cdr_block_number;
            $cdr_block = "--BEGIN CDR BLOCK--".$cdr_block_number."|".$line."|${cdr_parent_event_block_number}"."\n";
            #also reset $participant_id when a parent event starts
            $participant_id = undef;
            $cdr_parent_event_date = undef;
        }
        elsif($cdr_block !~ /^CDR_TICKET::/s && $cdr_block =~ /\n\n+$/s && $line =~ /^[A-Z0-9_'s\s]+$/ && $line =~ /CONTINUE|PARTICIPANT INFO/){
            #entered a child CDR event...child CDR events have the following strings...CONTINUE, PARTICIPANT INFO...
            if(!defined($participant_id)){
                #this will happen only when the first child starts or before the parent gets saved...else it will remain undef for all children...
                ($participant_id) = $cdr_block =~ /Participant ID:\s*(\d+)/;
            }
            my ($event_date) = $cdr_block =~ /--BEGIN CDR BLOCK--.+?\n(.+[A|P]M)\n/;
            if( !defined($cdr_parent_event_date) ){
                if(defined($event_date)){
                    $cdr_parent_event_date = $event_date;
                }
                else{
                    $cdr_parent_event_date = 'na';
                }
            }
            if(!defined($event_date)){
                $event_date = $cdr_parent_event_date;
                $cdr_block =~ s/(--BEGIN CDR BLOCK--.+?)\n/$1\|${event_date}\n/g;
            }
            else{
                $cdr_block =~ s/(--BEGIN CDR BLOCK--.+?)\n(.+[A|P]M)\n/$1\|$2\n/g;
            }
            if(!defined($participant_id)){
                $cdr_block =~ s/(--BEGIN CDR BLOCK--.+?)\n/$1\|\n/g;
            }
            else{
                $cdr_block =~ s/(--BEGIN CDR BLOCK--.+?)\n/$1\|${participant_id}\n/g;
            }
            $cdr_block =~ s/\n+$//s;
            $cdr_block =~ s/\n+/\n/gs;
            print FHW "$cdr_block\n"."--END CDR BLOCK--\n";
            $cdr_block_number++;
            #re-initialise the next cdr-block child...parent event should already have been initialised...  
            $cdr_block = "--BEGIN CDR BLOCK--".$cdr_block_number."|".$line."|${cdr_parent_event_block_number}"."\n";
        }
        else{
            #if the line is not an event just keep appending it to the current block...
            $cdr_block .= $line."\n";
        }
    }
    #print the last cdr-block to the output file :-)...
    if($cdr_block){
        if(!defined($participant_id)){
            #this will happen only if the last block is a parent without children and is a participant event...
            ($participant_id) = $cdr_block =~ /Participant ID:\s*(\d+)/;
        }
        my ($event_date) = $cdr_block =~ /--BEGIN CDR BLOCK--.+?\n(.+[A|P]M)\n/;
        if( !defined($cdr_parent_event_date) ){
            if(defined($event_date)){
                $cdr_parent_event_date = $event_date;
            }
            else{
                $cdr_parent_event_date = 'na';
            }
        }
        if(!defined($event_date)){
            $event_date = $cdr_parent_event_date;
            $cdr_block =~ s/(--BEGIN CDR BLOCK--.+?)\n/$1\|${event_date}\n/g;
        }
        else{
            $cdr_block =~ s/(--BEGIN CDR BLOCK--.+?)\n(.+[A|P]M)\n/$1\|$2\n/g;
        }
        if(!defined($participant_id)){
            $cdr_block =~ s/(--BEGIN CDR BLOCK--.+?)\n/$1\|\n/g;
        }
        else{
            $cdr_block =~ s/(--BEGIN CDR BLOCK--.+?)\n/$1\|${participant_id}\n/g;
        }
        $cdr_block =~ s/\n+$//s;
        $cdr_block =~ s/\n+/\n/gs;
        print FHW "$cdr_block\n"."--END CDR BLOCK--\n";
    }
    print time.":Preprocessed file=$preprocessed_file\n";
    close(FHW);
    close(FHR);
}