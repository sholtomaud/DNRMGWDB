=setup
[Configuration]
ListFileExtension = HTM

[Window]
Head = SQLITE3.HSC - Import GWDB to SQLite3 db 

[Labels]
FLD = END   20  2 Input Folder
REP     = END   +0 +1 Report File

[Fields]
FLD         = 21  2 INPUT   CHAR       60  0  TRUE   0.0 0.0 'S:\HYDSTRA\PROD\HYD\DAT\PTMP\GWDB\GWDB_20130926\DATA\                                     ' $PA
REP         = +0 +1 INPUT   CHAR       40  0  FALSE   TRUE   0.0 0.0 '#PRINT(P           )'

[Perl]

[Doco]
Import data from QLD DNRM Ground Water db .txt files to Hydstra.\n");
  - Developed for Arrow Energy Pty. Ltd. by Sholto Maud\n");
  \n");
  Notes:\n");
  1. This importer caches data in a provisional, dated SQLite database\n");
     for further processing\n");
  2. The SQLite db is located at [$sqlite3]\n");
  3. The import also performs some provisional cleansing of the data by \n");
       removing unclosed quote ("), (') characters that exist in the \n));
       DNRM GWDB files\n));
       \n));
       For Queries Contact: \n));
       e: sholto.maud\@gmail.com\n));
       m: +61(0)424 094 227\n));







=cut

use warnings;
use strict;

use Compress::Zlib;
use HyBootstrap; #requires use File::Copy
use File::Copy;
use Chromicon::Hydstra::Defaults;
use JSON;
use HydDllp;
use DBI;
use HydADO;
use JSON;
use Text::CSV_XS;

require 'hydlib.pl';
require 'hydtim.pl';



##############################################################################
# Globals
##############################################################################
#open new priv.hyvirtual table  
my ($hyconf,$data_source,@progress,%tscalc_wq,$dll,$repfile,%hydtables,%dbi);
my $WORKAREA = '[priv.gwdbimp]';
my $prtdest_scr   = '-S';   #the #Prt() print destination for screen messages
my $prtdest_rep   = '-R';   #the #Prt() print destination for screen messages
my $prtdest_log   = '-T';    #the #Prt() print destination for log messages
my $prtdest_debug = '-T';     #the #Prt() print destination for debug messages
my $prtdest_data  = '-T';     #the #Prt() print destination for the data hash
my %gwdb_file_naming_mappings;

sub RunJob {
  #run a job, log it, handle errors, print progress etc
  my ($job,$repfile,$progref)=@_;
  if (ref($progref) ne 'ARRAY') {
    #::Prt("-RSX","RunJob should be called with a third parameter of a pointer to a \@progress array, using a pass by reference\n");
  }
  if($job!~m{/hide}){
    #Prt('-RSX',"*** Job [$job] doesnt have /hide\n");
  }
  if($job!~m{/e=hyconf}){
    #Prt('-RSX',"*** Job [$job] doesnt have /e=hyconf\n");
  }
  
  #Prt('-RS',"\n",NowStr(),' ');
  PrintProgress('-RS',@$progref); 
  #Prt('-ST',"\n");
  if ( PrintAndRun( '-RLS',$job,0,1,$repfile) > 0 ) {
    PrintFile('-RLS',HyconfigValue('TEMPPATH').'hydsys.err');
    #Prt('-RLS',"*** Error returned by previous job step\n");
  }
  unlink($repfile);
  return;
}



main: {
  
  my $prog = FileName($0);
  my $start_time = NowRel();
  
  my $db_date = NowString();
  
  my $junkpath = HyconfigValue('JUNKPATH');
  my $ptmppath = HyconfigValue('PTMPPATH');
  
  my $dnrmgwdb_SQLite_dir   = $ptmppath.'\\DNRM\\GWDB\\PROCESSED\\GWDB_SQLite\\';
  my $dnrmgwdb_Hydstra_dir  = $ptmppath.'\\DNRM\\GWDB\\PROCESSED\\GWDB_Hydstra\\';
  
  MkDir($dnrmgwdb_SQLite_dir);
  MkDir($dnrmgwdb_Hydstra_dir);
  
  #my $hydstra_defaults   = getHyDefaults();
  #my $hydstra_keys       = getHyKeys();
  #my $dnrmgwdb_keys      = getDNRMGWDBKeys();
  #my $dnrmgwdb_datatypes = getDNRMGWDBDataTypes();
  
  my $dnrmgwdb_SQLite   = $dnrmgwdb_SQLite_dir.$db_date.'.db';
  my $dnrmgwdb_Hydstra  = $dnrmgwdb_Hydstra_dir.$db_date.'.db';
  
  my $sqlite3path = 'C:\\Hydstra\\temp\\sqlite'; 
  MkDir($sqlite3path);
  
  
  DBILoad(\%dbi); 
  #my $sqlite3 = "$sqlite3path.'\\GWDB_'.$db_date.'.db';
  my $sqlite3 = $sqlite3path.'\\GWDB_20131111081529.db';
  my $staging_db = $sqlite3path.'\\staging.db';
  
    
  my $nowdat = substr (NowString(),0,8); #YYYYMMDDHHIIEE to YYYYMMDD for default import date
  my $nowtim = substr (NowString(),8,4); #YYYYMMDDHHIIEE to HHII for default import time

  Prt($prtdest_scr,"##################################################################################\n");
  Prt($prtdest_scr,"##  \n");
  Prt($prtdest_scr,"##  Import data from QLD DNRM Ground Water db .txt files to Hydstra.\n");
  Prt($prtdest_scr,"##    - Developed for Arrow Energy Pty. Ltd. by Sholto Maud\n");
  Prt($prtdest_scr,"##    \n");
  Prt($prtdest_scr,"##    Notes:\n");
  Prt($prtdest_scr,"##    1. This importer caches data in a provisional, dated SQLite database\n");
  Prt($prtdest_scr,"##       for further processing\n");
  Prt($prtdest_scr,"##    2. The SQLite db is located at [$sqlite3]\n");
  Prt($prtdest_scr,"##    3. The import also performs some provisional cleansing of the data by \n");
  Prt($prtdest_scr,qq(##       removing unclosed quote ("), (') characters that exist in the \n));
  Prt($prtdest_scr,qq(##       DNRM GWDB files\n));
  Prt($prtdest_scr,qq(##       \n));
  Prt($prtdest_scr,qq(##       For Queries Contact: \n));
  Prt($prtdest_scr,qq(##       e: sholto.maud\@gmail.com\n));
  Prt($prtdest_scr,qq(##       m: +61(0)424 094 227\n));
  Prt($prtdest_scr,"##  \n");
  Prt($prtdest_scr,"##################################################################################\n\n");

  my %defaults = (
          aquifer  => {
                       hole => 1,
                      },
          casing  => {
                       hole => 1,
                       pipe => 1,
                      },
          gwhole   => {
                       hole => 1,
                      },
          history   => {
                       statdate   => $nowdat,
                       stattime   => $nowtim,
                       keyword    => 'NRMIMP'
                      },            
          lithdril   => {
                       hole => 1,
                       interpret => 'unknown',
                      },
          lithstra   => {
                       hole => 1,
                       interpret => 'unknown',
                       depthfrom => 0,
                       depthto => 0,
                      },
          gwpipe   => {
                       hole => 1,
                       pipe => 1,
                      },
          hydmeas  => {
                       hole => 1,
                       pipe => 1,
                       date => 19000101,
                       time => 0000,
                       quality => 50,
                       #variable => 110.00,         #only water levels are stored in HYMEAS, so variable is always 110
                      },
          hydrlmp  => {
                       hole => 1,
                       pipe => 1,
                       date => 19000101,
                       time => 0000,
                      },
          pumptest => {
                       hole => 1,
                       pipe => 1,
                       starttime => 0000,
                       timeoftest => 'NOT',
                      },
          gwtrace =>  {
                       hole => 1,
                       pipe => 1,
                       time => 0000,
                      },
          samples =>  {
                       bottle => 1,
                      },
          results =>  {
                       bottle => 1,
                       quality => 50,
                      },
          gwtracer => {
                       hole => 1,
                       pipe => 1,
                       time => 0000,
                       quality => 50,
                      },
          site     => {
                       active => 'T',
                       orgcode => 'QWR',
                       category5 => 'GWDB',
                      },
         );
  
=skip  
  my %primary_keys = (
          aquifer_data                  => { rn => 1, rec => 1, top => 1 },
          casing_data                   => { rn => 1, rec => 1, pipe => 1, top => 1, out_diameter => 1 , rdate => 1 },
          elevation_data                => { rn => 1, pipe => 1, rdate => 1 },
          facility_roles_data           => { rn => 1 , facility_role => 1 },
          field_waterqual_data          => { rn => 1, pipe => 1, rdate => 1 },
          multiple_conductivity_data    => { rn => 1, pipe => 1, rdate => 1, depth => 1 },
          gw_regdets                    => { rn => 1 },
          sample_variables              => { variable_no => 1 },
          strata_log_data               => { rn => 1, rec => 1, top => 1 },
          stratigraphy_data             => { rn => 1, rec => 1, top => 1 },
          water_analysis_data           => { rn => 1, rec => 1, pipe => 1, rdate => 1 },
          water_levels                  => { rn => 1, pipe => 1, rdate => 1 },
          water_samples_results_data    => { rn => 1, pipe => 1, sampnum => 1, bottle => 1, variable_no => 1 },
          water_samples_data            => { rn => 1, pipe => 1, sampnum => 1, bottle => 1  },
  );
=cut
=skip  
  my  %data_type_mapping = (
                  aquifer_data =>      [#AQUIFER.txt
                                  #"RN"|"REC"|"CONDITION"|"TOP"|"BOTTOM"|"CONTR"|"FLOW"|"QUALITY"|"YIELD"|"SWL"|"RDATE"|"FORM_DESC"
                                  #65054|1|UC|22.5|23.5|||||||ELLIOTT
                                  { rn => 'numeric'},
                                  { rec => 'text'}, #not used.
                                  { condition => 'text'}, #DESCRIPTION CODE, Porous Rocks: Unconsolidated UC,Consolidated PS,Semi-Consolidated SC,Fractured Rocks:Fractured FR,Vesicular VS,Cavernous CV,Weathered Zone WZ
                                  { top => 'real'},
                                  { bottom => 'real'},
                                  { contr => 'text'},
                                  { flow => 'blob'},
                                  { quality => 'text'},
                                  { yield => 'real'},
                                  { swl => 'real'},
                                  { rdate => 'text'},
                                  { form_desc => 'text'}
                                ],
                  casing_data  =>     [#CASINGS.txt
                                  #"RN"|"PIPE"|"RDATE"|"REC"|"MATERIAL_DESC"|"MATERIAL_SIZE"|"SIZE_DESC"|"OUT_DIAMETER"|"TOP"|"BOTTOM"
                                  #65174|A|23/11/1986|1|PLAS|5|WT|110|0|19.8 
                                  #all casings are depth below ground level
                                  {rn => 'numeric'},
                                  {pipe => 'text'}, 
                                  {rdate => 'text'}, 
                                  {rec => 'text'}, #not used.
                                  {material_desc => 'text'}, 
                                  {material_size => 'real'}, 
                                  {size_desc => 'text'}, #dummy placeholder to be processed in special cases 
                                  {out_diameter => 'real'}, 
                                  {top => 'real'}, 
                                  {bottom => 'real'} 
                                 ],
                  elevation_data =>   [#ELEVATIONS.txt
                                  #"RN"|"PIPE"|"RDATE"|"MEAS_POINT"|"PRECISION"|"DATUM"|"ELEVATION"|"SURVEY_SOURCE"
                                  #22013|X|01/01/1923|N|EST|STD|16.7|  
                                  #A B C for pipes = R for reference point}, N then is will be pipe X which is ground surface
                                  {rn => 'numeric'}, 
                                  {pipe => 'text'}, 
                                  {rdate => 'text'}, 
                                  {meas_point => 'text'}, 
                                  {precision => 'text'}, 
                                  {datum => 'text'}, 
                                  {elevation => 'numeric'}, 
                                  {survey_source => 'text'} 
                                 ],                             
                  facility_roles_data =>[#FACILITY_ROLES.txt
                                  #"RN"|"FACILITY_ROLE"|"COMMENTS"
                                  #65912|WS| 
                                  {rn => 'numeric'},
                                  {facility_role => 'text'},
                                  {comments => 'text'}
                                 ],
                  field_waterqual_data => [ #FIELD_WATER_QUALITY.txt
                                  #"RN"|"PIPE"|"RDATE"|"SAMP_METHOD"|"SOURCE"|"DEPTH"|"CONDUCT"|"DO2"|"EH"|"NO3"|"PH"|"TEMP"|"ALKALINITY"
                                  #40044|A|22/12/1976||GB||183||||||
                                  {rn => 'numeric'},
                                  {pipe => 'text'}, 
                                  {rdate => 'text'}, 
                                  {samp_method => 'text'},
                                  {source => 'text'},
                                  {depth => 'real'}, #Bore water level
                                  {conduct => 'real'}, 
                                  {do2 => 'real'},
                                  {eh => 'real'}, #EH = REDOX
                                  {no3 => 'real'}, 
                                  {ph => 'real'},
                                  {temp => 'real'},
                                  {alkalinity => 'real'} 
                                 ],
                  multiple_conductivity_data => [#MULTIPLE_COND.txt
                                  #"RN"|"PIPE"|"RDATE"|"DEPTH"|"MEASUREMENT"|"TEMP"
                                  #13600158|A|11/04/1990|-3|39400| 
                                  {rn => 'numeric'}, 
                                  {pipe => 'text'},
                                  {rdate => 'text'},  
                                  {depth => 'real'}, 
                                  {measurement => 'real'},  
                                  {temp => 'real'}  
                                 ],
                  gw_regdets => [#REGISTRATIONS.txt or #GW_REGDETS.TXT
                                  #"RN"|"FACILITY_TYPE"|"OFFICE"|"SHIRE_CODE"|"PARISH"|"RN_REPLACES"|"DO_FILE"|"RO_FILE"|"HO_FILE"|"FACILITY_STATUS"|"DRILLED_DATE"|"DRILLER_NAME"|"DRILLING_COMP"|"BASIN"|"METHOD_CONST"|"SUB_AREA"|"LOT"|"PLAN"|"DESCRIPTION"|"COUNTY"|"PROPERTY_NAME"|"LAT"|"LNG"|"EASTING"|"NORTHING"|"ZONE"|"ACCURACY"|"GPS_ACCURACY"|"GIS_LAT"|"GIS_LNG"|"CHECKED"|"MAP_SCALE"|"MAP_SERIES"|"MAP_NO"|"PROG_SECT"|"EQUIPMENT"|"ORIG_NAME_NO"|"POLYGON"|"CONFIDENTIAL"|"DATA_OWNER"|"BORE_LINE_CODE"|"DRILLER_LICENCE_NUMBER"|"LOG_RECEIVED_DATE"|"SHAPE"
                                  #19|SF|BBG|6630|1039||||||01/01/1908|||1363|||||P75V|FITZROY||||||56||||||104|M|9245|||O/S PROCLAIMED AREA|||DNR||||
                                  {rn => 'numeric'},
                                  {facility_type => 'text'},
                                  {office => 'text'},
                                  {shire_code => 'numeric'},
                                  {parish => 'text'},
                                  {rn_replaces => 'text'},
                                  {do_file => 'text'},
                                  {ro_file => 'text'},
                                  {ho_file => 'text'},
                                  {facility_status => 'text'},
                                  {drilled_date => 'text'}, #do I need to have pumptest??
                                  {driller_name => 'text'},
                                  {drilling_comp => 'text'},
                                  {basin => 'text'},
                                  {method_const => 'text'},
                                  {sub_area => 'text'},
                                  {lot => 'blob'},
                                  {plan => 'text'},
                                  {description => 'text'},
                                  {county => 'text'},
                                  {property_name => 'text'},
                                  {lat => 'real'},
                                  {lng => 'real'},
                                  {easting => 'real'},
                                  {northing => 'real'},
                                  {zone => 'text'},
                                  {accuracy => 'text'},
                                  {gps_accuracy => 'real'},
                                  {gis_lat => 'real'},
                                  {gis_lng => 'real'},
                                  {checked => 'text'},
                                  {map_scale => 'text'},
                                  {map_series => 'text'},
                                  {map_no => 'text'},
                                  {prog_sect => 'text'},
                                  {equipment => 'text'},
                                  {orig_name_no => 'text'},
                                  {polygon => 'text'},
                                  {confidential => 'text'},
                                  {data_owner => 'text'},
                                  {bore_line_code => 'text'},
                                  {driller_licence_number => 'text'},
                                  {log_received_date => 'text'},
                                  {shape => 'text'}
                                ],
                                
                  sample_variables =>   [#STRATA_LOG_DATA.txt
                                  #"VARIABLE_NO"|"NAME"|"UNIT_CODE"|"UNITS"|"SHORTNAME"|"COMMNT"|"MINIMUM"|"MAXIMUM"|"ACCURACY"|"PRECISION"|"REPORTFORM"|"ELECCHARGE"|"MOLAREQUIV"|"CASEREF"
                                  #766.24|Total Runoff  (event)|MM|mm|Total Runoff|event based|0|0|0|.00001|LEVEL||0|
                                  {'variable_no'=> 'numeric'},
                                  {'name'=> 'text'},
                                  {'unit_code'=> 'text'},
                                  {'units'=> 'text'},
                                  {'shortname'=> 'text'},
                                  {'commnt'=> 'text'},
                                  {'minimum'=> 'numeric'},
                                  {'maximum'=> 'numeric'},
                                  {'accuracy'=> 'blob'},
                                  {'precision'=> 'numeric'},
                                  {'reportform'=> 'text'},
                                  {'eleccharge'=> 'text'},
                                  {'molarequiv'=> 'text'},
                                  {'caseref' => 'text'}
                                  ],
                  strata_log_data =>   [#STRATA_LOG_DATA.txt
                                  #"RN"|"REC"|"DESCR"|"TOP"|"BOTTOM"
                                  #95987|1|TOPSOIL|0|.3
                                  #rn => 'site.station,gwhole.station,lithdril.station,', 
                                  {rn => 'numeric'}, 
                                  {rec => 'numeric'}, 
                                  {descr => 'text'}, 
                                  {top => 'numeric'}, 
                                  {bottom => 'numeric'}
                                  ],
                  stratigraphy_data =>  [#STRATIGRAPHY_DATA.txt
                                  #"RN"|"REC"|"DATA_OWNER"|"FORM_DESC"|"TOP"|"BOTTOM"
                                  #13700092|1|DNR|ELLIOTT FORMATION|0|18.9
                                  {rn => 'numeric'}, 
                                  {rec => 'text'},  
                                  {data_owner => 'text'},  
                                  {form_desc => 'text'},  
                                  {top => 'real'}, 
                                  {bottom => 'real'}
                                 ],
                               
                  water_analysis_data =>[#WATER_ANALYSIS_DATA.txt
                                  #"RN"|"PIPE"|"RDATE"|"REC"|"ANALYST"|"ANALYSIS_NO"|"SAMP_METHOD"|"SOURCE"|"PRESMETH1"|"COLLSAMP"|"PROJECT1"|"DEPTH"|"CONDUCT"|"PH"|"COLOUR"|"COLOUR_IND"|"TURB"|"TURB_IND"|"SIO2"|"SIO2_IND"|"HARD"|"ALK"|"ALK_IND"|"FIG_MERIT"|"NA_ADS_RATIO"|"RES_ALK"|"TOTAL_IONS"|"TOTAL_SOLIDS"|"NA"|"NA_IND"|"K"|"K_IND"|"CA"|"CA_IND"|"MG"|"MG_IND"|"FE"|"MN"|"FE_IND"|"MN_IND"|"HCO3"|"HCO3_IND"|"CO3"|"CO3_IND"|"CL"|"CL_IND"|"F"|"F_IND"|"NO3"|"NO3_IND"|"SO4"|"SO4_IND"|"ZN"|"ZN_IND"|"AL"|"AL_IND"|"B"|"B_IND"|"CU"|"CU_IND"|"PO4"|"PO4_IND"|"BR"|"BR_IND"|"I"|"I_IND"
                                  #19|A|11/09/1986|1|GCL|116325||GB||||0|2050|8|||||32||860|325||4.7|1.3|0|1290|1120|85||1.8||180||100||.08|3.1|||390||3.2||510||.2||.9||18||||||||||0||||| 
                                  {rn => 'numeric'},
                                  {pipe => 'text'}, 
                                  {rdate => 'text'}, 
                                  {rec => 'text'},
                                  {analyst => 'text'},
                                  {analysis_no => 'text'},
                                  {samp_method => 'text'},
                                  {source => 'text'},
                                  {presmeth1 => 'text'},
                                  {collsamp => 'text'},
                                  {project1 => 'text'},
                                  {depth => 'real'},
                                  {conduct => 'real'}, 
                                  {ph => 'real'},
                                  {colour => 'text'},
                                  {colour_ind => 'text'},
                                  {turb => 'real'},
                                  {turb_ind => 'text'},
                                  {sio2 => 'real'},
                                  {sio2_ind => 'text'},
                                  {hard => 'real'},
                                  {alk => 'real'},
                                  {alk_ind => 'text'},
                                  {fig_merit => 'real'},
                                  {na_ads_ratio => 'real'},
                                  {res_alk => 'real'},
                                  {total_ions => 'real'},
                                  {total_solids => 'real'},
                                  {na => 'real'},
                                  {na_ind => 'text'},
                                  {k => 'real'},
                                  {k_ind => 'text'},
                                  {ca => 'real'},
                                  {ca_ind => 'text'},
                                  {mg => 'real'},
                                  {mg_ind => 'text'},
                                  {fe => 'real'},
                                  {mn => 'real'},
                                  {fe_ind => 'text'},
                                  {mn_ind => 'text'},
                                  {hco3 => 'real'},
                                  {hco3_ind => 'text'},
                                  {co3 => 'real'},
                                  {co3_ind => 'text'},
                                  {cl => 'real'},
                                  {cl_ind => 'text'},
                                  {f => 'real'},
                                  {f_ind => 'text'},
                                  {no3 => 'real'},
                                  {no3_ind => 'text'},
                                  {so4 => 'real'},
                                  {so4_ind => 'text'},
                                  {zn => 'real'},
                                  {zn_ind => 'text'},
                                  {al => 'real'},
                                  {al_ind => 'text'},
                                  {b => 'real'},
                                  {b_ind => 'text'},
                                  {cu => 'real'},
                                  {cu_ind => 'text'},
                                  {po4 => 'real'},
                                  {po4_ind => 'text'},
                                  {br => 'real'},
                                  {br_ind => 'text'},
                                  {i => 'real'},
                                  {i_ind => 'text'}
                                  ],
                  water_levels =>  [#WATER_LEVELS.txt
                                  #"RN"|"PIPE"|"RDATE"|"MEAS_POINT"|"MEASUREMENT"|"REMARK"|"LOGGER"
                                  #13600011|A|24/06/1981|R|-17.28|| 
                                  {rn => 'numeric'}, 
                                  {pipe => 'text'}, 
                                  {rdate => 'text'}, 
                                  {meas_point => 'text'}, 
                                  {measurement => 'real'}, 
                                  {remark => 'text'}, 
                                  {logger => 'text'} 
                                  ],
                  water_samples_results_data =>    [#WQ_SAMPLES_RESULTS_DATA.txt
                                  #"RN"|"PIPE"|"SAMPNUM"|"BOTTLE"|"VARIABLE_NO"|"FLAG"|"VALUE"|"QUALITY"|"COMMNT"
                                  #12600532|A|182289|B|2741.2|<|.004|| 
                                  #I believe this results table needs to have a key that is in accordance with the 
                                  #water_samples_data file.
                                  {rn => 'numeric'},
                                  {pipe => 'text'},
                                  {sampnum => 'text'},
                                  {bottle => 'text'},
                                  {variable_no => 'real'},
                                  {flag => 'text'},
                                  {value => 'real'},
                                  {quality => 'text'},
                                  {commnt => 'text'}
                                 ],
                  water_samples_data =>    [#WQ_SAMPLES_DATA.txt
                                  #"RN"|"PIPE"|"SAMPNUM"|"BOTTLE"|"SDATE"|"PROJECT1"|"SRCSAMP"|"COLLSAMP"|"COLLMETH"|"PRESMETH1"|"REC"|"DEPTH"|"PROJECT2"|"PROJECT3"|"PROJECT4"|"PRESMETH2"|"PRESMETH3"|"PRESMETH4"|"LABREF"|"LABSAMPREF"|"RECDATE"|"INPUTSRC"|"ENTEREDBY"|"CHECKEDBY"|"COMMNT"
                                  #13610198|A|210873|D|22/07/2002|GWAN|GB|DG|PW|FR|1|28|||||||GCL||01/08/2002||GW||
                                  {rn => 'numeric'},
                                  {pipe => 'text'},
                                  {sampnum => 'text'},
                                  {bottle => 'text'},
                                  {sdate => 'text'},
                                  {project1 => 'text'},
                                  {srcsamp => 'text'},
                                  {collsamp => 'text'},
                                  {collmeth => 'text'},
                                  {presmeth1 => 'text'},
                                  {rec => 'text'},
                                  {depth => 'numeric'},#set the depth and deal with it as a special case
                                  {project2 => 'text'},
                                  {project3 => 'text'},
                                  {project4 => 'text'},
                                  {presmeth2 => 'text'},
                                  {presmeth3 => 'text'},
                                  {presmeth4 => 'text'},
                                  {labref => 'text'},
                                  {labsampref => 'text'},
                                  {recdate => 'text'},
                                  {inputsrc => 'text'},
                                  {enteredby => 'text'},
                                  {checkedby => 'text'},
                                  {commnt => 'text'}
                                ]
  );
=cut
                          
  
  my (%params,%ini);
  my %report;
  IniCrack($ARGV[0],\%params);
  #IniCrack('are.sqlite3.test.ini',\%params);
  IniHash('are.sqlite3.test.ini',\%ini);
   my $dll;
  $dll = HydDllp->New('','');
  my $ref;
  
  #get the filename configs from INI and push them to the $gwdb_file_naming_mappings hash
  Prt($prtdest_scr,NowStr()." $prog started\n");
  Prt($prtdest_scr,NowStr()."  - Getting filename configs from INI\n");
  foreach my $fileconfig ( keys %{$ini{'config.filenames'}} ){
    #Prt('-P',"file config [$fileconfig]\n");
    #Prt('-P',"Past the file config [$ini{'config.filenames'}{$fileconfig} ]" );
    my @filenames  = split(/,/,$ini{'config.filenames'}{$fileconfig});
    foreach my $filename (@filenames){
      $gwdb_file_naming_mappings{$filename} = $fileconfig ;
    }
  }

  #query available drivers
  #my @ary = DBI->available_drivers();
  #make connection to db
  Prt($prtdest_scr,NowStr()."  - Connecting to $sqlite3\n");
  my $dbh = DBI->connect(          
      "dbi:SQLite:dbname=$sqlite3", 
      "",                          
      "",                          
      { RaiseError => 1, AutoCommit => 0},         
  ) or die $DBI::errstr;
  
  #get GWDB data files
  my $datafolder = $params{'perl_parameters_fld'};
  Prt($prtdest_scr,NowStr()."  - Retrieving data files from [$datafolder]\n");
  my @GWDB_data_files = DOSFileList($datafolder,0);
  Prt($prtdest_scr,NowStr()."   - Found [$#GWDB_data_files] .txt db files\n");
  my @tables;
  
  #my $tablefields = $mappings{$filekey}{$gwdb_fieldname};
  
  
  
  Prt('-P',HashDump(\%{$ini{'config.filenames'}}));
  #Prt('-P',HashDump(\%{$ini{'dnrmgwdb.primarykeys'}}));
  
  foreach my $file ( sort keys %{$ini{'config.filenames'}}){
  
    Prt('-S',"dapa [$file] [$ini{'config.filenames'}{$file}]");
    
    $ini{'config.filenames'}{$file} =~ s{\"}{}g;
    my $fl = $ini{'config.filenames'}{$file};
    
    #Prt('-P',HashDump(\@{$fl}));
    
  }
  
  #foreach my $primary_keys ( keys %{$ini{'dnrmgwdb.primarykeys'}}){
    
    
    #while (my ($key, $val) = each %$primary_keys) {
    #  Prt('-P', "$key=>$val\n");
    #
    #}
  #}
  
  
  
  ##############!!!!!!!!!!!!!!!!!!! DON"T UPDATE SITE IF IT HAS A BASELINE ASSESSMENT !!!!!!!!!!!!!!!!!!!!!!!!!!!!
  
=skip  
  foreach my $GWDB_file (@GWDB_data_files){
    my $field_count = 0;  
    my $GWDB_filename = lc(FileName($GWDB_file));  #the key used to index the %GWDB_files hash, based on the CSV GWDB_file
    my $GWDB_file_key = $gwdb_file_naming_mappings{$GWDB_filename};  #DNRM don't use consistent file naming so need to handle      
    
    #Check that the file is mapped to the standard name
    Prt($prtdest_scr,NowStr()."  - Checking [$GWDB_filename]\n");
    if ( !defined  ( $gwdb_file_naming_mappings{$GWDB_filename}   ) ){
      Prt('-XL',"*** ERROR The filename [$GWDB_file] is not defined in the config.filenames section of INI. Stopped import. Please assign a filename mappin gin the INI file and then re-run import.\n");
    }
    #Also check that the file is defined in the data type mappings hash
    elsif ( !defined  ( $data_type_mapping{ $gwdb_file_naming_mappings{$GWDB_filename} } ) ){
      Prt('-XL',"***ERROR - There is no mapping for [$GWDB_file] in the data_type_mapping hash. Stopped import. Please fix the data_type_mapping hash in and rerun the importer\n")
    } 
    else{
      Prt($prtdest_scr,NowStr()."   - File name is recognised in INI config and defined with data type mappings\n");
      my $table = $gwdb_file_naming_mappings{$GWDB_filename};
      
      my $total_primary_keys = keys %{$primary_keys{lc($table)}};
      my $primary_keys_count = 0;
      
      my $primary_key = 'PRIMARY KEY (';
      
      Prt($prtdest_scr,NowStr()."   - Creating primary key\n");
      
      
      
      my $primary_keys = $ini{dnrmgwdb.primarykeys};
      
      
      
      foreach my $primary_ky ( keys %{$primary_keys{lc($table)}} ){
        $primary_keys_count++;
        $primary_key .= "$primary_ky, ";
        ($total_primary_keys == $primary_keys_count) ? $primary_key =~ s{\, $}{\)}:next;
      }
      
      my $uctable = ucfirst($table);
      push (@tables,$uctable);
      my $lctable = lc($table);
      my $hash_header_key;
      my %table_column_datatype;
      #$dbh->do("DROP TABLE IF EXISTS $sqlite3.$uctable");
      my $total_datatype_columns = scalar @{$data_type_mapping{$table}};
      ##Prt('-P',"lctable [$lctable], ucftable [$uctable]\n");
      #if ( $lctable ne 'aquifer_data' && $lctable ne 'casing_data' ){
      #if ( $lctable eq 'facility_roles_data' || $lctable eq 'elevations'){
      #Prt($prtdest_scr,NowStr()."   - generating CREATE statement for [$lctable]                     \n");
      my $create;# = 'id INTEGER PRIMARY KEY AUTOINCREMENT';
      my $vals = '';
      
      foreach my $order ( @{$data_type_mapping{$table} } ){
        ##Prt('-P',"order [".HashDump(\%{$order})."]\n");
        #my %ordr = %{$order};
        foreach my $field ( keys %{$order} ){
          $field_count++;
          
          ##Prt('-P',"field [$field]\n");
          my $fld   = ucfirst($field);
          my $ucfld = uc($field);
          $hash_header_key .= qq{"$ucfld"|};
          my $data_type    = uc($$order{$field});
          $table_column_datatype{$lctable}{$field_count} = $data_type;
          
          $create .= " $fld $data_type, ";
          ##Prt('-P',"field [$fld], data type [$data_type], Create [$create]\n");
          $hash_header_key =~ s{\|$}{} if ($total_datatype_columns == $field_count);
          
          $vals .= "?, ";
          ( $field_count == $total_datatype_columns )? $vals =~ s{\, $}{} : '';
          
          ($total_datatype_columns == $field_count) ? $create =~ s{\, $}{}:next;
        }
      }
      ##Prt('-L',"Fld count [$field_count]\n total dataypes [$total_datatype_columns]\n");

      
      #$dbh->do("DROP TABLE IF EXISTS $uctable");
      $dbh->do("CREATE TABLE IF NOT EXISTS $uctable ($create, $primary_key)");
      Prt($prtdest_scr,NowStr()."    - Success\n");
      Prt($prtdest_scr,NowStr()."   - Processing rows\n");
      my $sth = $dbh->prepare("INSERT INTO $uctable VALUES ($vals)");
      
      #Setup Text::CSV_XS 
      my $csv = Text::CSV_XS->new ({ 
        sep_char => '|', 
        escape_char => '', 
        quote_char =>'', 
        allow_loose_quotes =>1 , 
        always_quote =>1 });
      
      #Open .txt file 
      open my $io, "<:encoding(utf8)", $GWDB_file or die "$GWDB_file : $!";
      #Read each line in the file and store in the journal 
      my $count = 0;
      my $file_processing_start = NowRel();
      while (my $row = $csv->getline ($io)) {
          $count++;
          #get rid of annoying unmatched quotes etc.
          map(s{["'\\]}{}g, @{$row});
          ($count == 1) ? next :  $sth->execute(@{$row}) or die $sth->errstr;
          print "  - Processing row [$count]  \r"; 
      }
      my $file_processing_end = NowRel();
      my $file_processing_total = $file_processing_end - $file_processing_start;
      
      $report{$GWDB_file}{rows} = $count;
      $report{$GWDB_file}{total_process_time} = ReltoTmp($file_processing_total,"II:EE").' (min:sec)';
      $report{$GWDB_file}{seconds_per_row} = $file_processing_total/$count;
      
      Prt($prtdest_scr,NowStr()."    - Commiting to temporary data file.\n");
      close $io;
      $dbh->commit;
      
      my $file_commit_end = NowRel();
      my $file_commit_total = $file_commit_end - $file_processing_end;
      $report{$GWDB_file}{total_commit_time} = ReltoTmp($file_commit_total,"II:EE").' (min:sec)';
      Prt($prtdest_scr,NowStr()."    - Done. Process time: $report{$GWDB_file}{total_reltime}, Sec/row: $report{$GWDB_file}{seconds_per_row}, commit time: $report{$GWDB_file}{total_commit_time}\n");
    }
  }
=cut
  
  
  #Prt('-P',"\n-------START CREATE VIEWS-------\n");
  #------------------------------
  #CREATE VIEWS
  #------------------------------
  
  #AQUIFER view
  #------------------------------
=skip  
  aquifer_data =>      {#AQUIFER.txt
                                  #"RN"|"REC"|"CONDITION"|"TOP"|"BOTTOM"|"CONTR"|"FLOW"|"QUALITY"|"YIELD"|"SWL"|"RDATE"|"FORM_DESC"
                                  #65054|1|UC|22.5|23.5|||||||ELLIOTT
                                  rn => 'aquifer.station,gwhole.station',
                                  rec => 'aquifer.rec', #not used.
                                  top => 'aquifer.depthfrom',
                                  bottom => 'aquifer.depthto',
                                  form_desc => 'aquifer.name',
                                  condition => 'aquifer.spare1', #DESCRIPTION CODE, Porous Rocks: Unconsolidated UC,Consolidated PS,Semi-Consolidated SC,Fractured Rocks:Fractured FR,Vesicular VS,Cavernous CV,Weathered Zone WZ
                                  contr => 'aquifer.spare5',
                                  flow => 'aquifer.spare4',
                                  quality => 'aquifer.comment',
                                  yield => 'aquifer.yield',
                                  swl => 'aquifer.swlvalue',
                                  rdate => 'aquifer.spare3',
=cut  
  #SELECT RN, TOP, BOTTOM, FORM_DESC, CONDITION, CONTR, FLOW, QUALITY, YIELD, SWL, RDATE FROM AQUIFER INNER JOIN AQUIFER ON AQUIFER.RN = GW_REGDETS.RN 
  
  #$dbh->do("CREATE VIEW AQUIFER_VIEW AS SELECT Rn, TOP, BOTTOM, FORM_DESC, CONDITION, CONTR, FLOW, QUALITY, YIELD, SWL, RDATE FROM AQUIFER_DATA INNER JOIN GW_REGDETS ON AQUIFER_DATA.ID = GW_REGDETS.RN ");
  
  
  #SITE view
  #------------------------------
  #"RN"|"FACILITY_TYPE"|"OFFICE"|"SHIRE_CODE"|"PARISH"|"RN_REPLACES"|"DO_FILE"|"RO_FILE"|"HO_FILE"|"FACILITY_STATUS"|"DRILLED_DATE"|"DRILLER_NAME"|"DRILLING_COMP"|"BASIN"|"METHOD_CONST"|"SUB_AREA"|"LOT"|"PLAN"|"DESCRIPTION"|"COUNTY"|"PROPERTY_NAME"|"LAT"|"LNG"|"EASTING"|"NORTHING"|"ZONE"|"ACCURACY"|"GPS_ACCURACY"|"GIS_LAT"|"GIS_LNG"|"CHECKED"|"MAP_SCALE"|"MAP_SERIES"|"MAP_NO"|"PROG_SECT"|"EQUIPMENT"|"ORIG_NAME_NO"|"POLYGON"|"CONFIDENTIAL"|"DATA_OWNER"|"BORE_LINE_CODE"|"DRILLER_LICENCE_NUMBER"|"LOG_RECEIVED_DATE"|"SHAPE"
  
  
  #$dbh->do("CREATE VIEW SITE_VIEW AS SELECT RN, FACILITY_TYPE, LAT, LNG FROM GW_REGDETS");
  
  #STRATIGRAPHY_DATA view
  #------------------------------
  #"RN"|"REC"|"DATA_OWNER"|"FORM_DESC"|"TOP"|"BOTTOM"
  #$dbh->do("CREATE VIEW STRATIGRAPHY_VIEW AS SELECT RN, TOP, BOTTOM FROM STRATIGRAPHY_DATA");
  
  #my $stp = $dbh->prepare( "SELECT * FROM SITE_VIEW LIMIT 10" );  
  #my $stp = $dbh->prepare( "SELECT * FROM AQUIFER_DATA LIMIT 5" );  
  #my $stp = $dbh->prepare( "SELECT * FROM STRATIGRAPHY_VIEW LIMIT 500" );  
  #$stp->execute();
  
  #Prt('-L',"\n-------START DATA-------\n");
  
#  my $statement = "SELECT * FROM GW_REGDETS WHERE LAT LIKE '30%' LIMIT 50";
=skip  
    my %data_model = (
          site => {
                    ,
                      keys  =>  {
                                station         => 1
                                },
                      tables =>  { 
                                    Gw_regdets    => {
                                          fields  =>  {
                                                rn => 'station',
                                                facility_type   => 'stntype', 
                                                basin           => 'category1',
                                                lat             => 'latitude',
                                                lng             => 'longitude',
                                                easting         => 'easting',
                                                northing        => 'northing',
                                                zone            => 'zone',
                                                accuracy        => 'posacc',
                                                gps_accuracy    => 'posacc',
                                                gis_lat         => 'category14',
                                                gis_lng         => 'category15',
                                                description     => 'comment',
                                                map_no          => 'mapname',
                                                data_owner      => 'owner'
                                          } 
                                        }  
                                }
                  }
  
  );
=cut  
  
  
  my %select = ( 
              'aquifer' => { 
                    'select' => qq(SELECT aquifer_data.RN as 'station', aquifer_data.TOP as 'depthfrom', aquifer_data.BOTTOM as 'depthto', aquifer_data.FORM_DESC as 'name', aquifer_data.CONDITION as 'spare1', aquifer_data.CONTR as 'spare5', aquifer_data.FLOW as 'spare4', aquifer_data.QUALITY as 'comment', aquifer_data.YIELD as 'yield' FROM AQUIFER_DATA left outer join GW_REGDETS on GW_REGDETS.RN=AQUIFER_DATA.RN where GW_REGDETS.RN=AQUIFER_DATA.RN ),
                    'keys' => ('station','depthfrom')
              },
              'site' => { 
                    'select' =>  qq(SELECT Gw_regdets.RN as 'station', Gw_regdets.FACILITY_TYPE as 'stntype', Gw_regdets.BASIN as 'category1',  Gw_regdets.ZONE as 'zone', Gw_regdets.ACCURACY as 'posacc', Gw_regdets.GPS_ACCURACY as 'posacc', Facility_roles_data.facility_role as 'category6', Gw_regdets.GIS_LAT as 'category14', Gw_regdets.GIS_LNG as 'category15', Gw_regdets.DESCRIPTION as 'comment', Gw_regdets.MAP_NO as 'mapname', Gw_regdets.DATA_OWNER as 'owner', Elevation_data.ELEVATION as 'elev', Elevation_data.PRECISION as 'elevacc' FROM GW_REGDETS left outer join ELEVATION_DATA on ELEVATION_DATA.RN=GW_REGDETS.RN left outer join FACILITY_ROLES_DATA on FACILITY_ROLES_DATA.RN=GW_REGDETS.RN where Elevation_data.MEAS_POINT = "N" and FACILITY_ROLES_DATA.RN=GW_REGDETS.RN),
                    'keys' => 'station'
              },
              'aresite' => { 
                    'select' =>  qq( SELECT Gw_regdets.RN as 'station', Gw_regdets.LOT as 'lot', Gw_regdets.PLAN as 'plan' FROM GW_REGDETS  ),
                    'keys' => 'station'
              },
              'gwhole' => { 
                    'select' => qq(SELECT Gw_regdets.RN as 'station', Gw_regdets.facility_status as 'conststat', Gw_regdets.drilled_date as 'startdate', Gw_regdets.method_const as 'constmeth' FROM GW_REGDETS left outer join GW_REGDETS on GW_REGDETS.RN=STRATA_LOG_DATA.RN where GW_REGDETS.RN=STRATA_LOG_DATA.RN),
                    'keys' => ('station','hole')
              },
              'lithdril' => { 
                    'select' => qq(SELECT strata_log_data.rn as 'station', strata_log_data.rec as 'rec', strata_log_data.descr as 'comment', strata_log_data.top as 'depthfrom', strata_log_data.bottom as 'depthto' FROM STRATA_LOG_DATA left outer join GW_REGDETS on GW_REGDETS.RN=STRATA_LOG_DATA.RN where GW_REGDETS.RN=STRATA_LOG_DATA.RN ),
                    'keys' => ('station','depthfrom')
              },
              'lithstra' => { 
                    'select' => qq(SELECT stratigraphy_data.rn as 'station,lithstra.station', stratigraphy_data.rec as 'rec', stratigraphy_data.data_owner as 'person', stratigraphy_data.form_desc as 'comments', stratigraphy_data.top as 'depthfrom', stratigraphy_data.bottom as 'depthto' FROM STRATIGRAPHY_DATA left outer join GW_REGDETS on GW_REGDETS.RN=STRATIGRAPHY_DATA.RN where GW_REGDETS.RN=STRATIGRAPHY_DATA.RN ),
                    'keys' => ('station','depthfrom')
              },    
              'samples' => {
                    'select' => qq(SELECT water_samples_data.rn as 'station', water_samples_data.pipe as 'spare3', water_samples_data.sampnum as 'sampnum', water_samples_data.bottle as 'bottle', water_samples_data.sdate as 'date', water_samples_data.project1 as 'project1', water_samples_data.srcsamp as 'srcsamp', water_samples_data.collsamp as 'collsamp', water_samples_data.collmeth as 'collmeth', water_samples_data.presmeth1 as 'presmeth1', water_samples_data.project2 as 'project2', water_samples_data.project3 as 'project3', water_samples_data.project4 as 'project4', water_samples_data.presmeth2 as 'presmeth2', water_samples_data.presmeth3 as 'presmeth3', water_samples_data.presmeth4 as 'presmeth4', water_samples_data.labref as 'labref', water_samples_data.labsampref as 'labsampref', water_samples_data.recdate as 'recdate', water_samples_data.inputsrc as 'inputsrc', water_samples_data.enteredby as 'enteredby', water_samples_data.checkedby as 'checkedby', water_samples_data.commnt as 'samples' FROM WATER_SAMPLES_DATA left outer join GW_REGDETS on GW_REGDETS.RN=WATER_SAMPLES_DATA.RN where GW_REGDETS.RN=WATER_SAMPLES_DATA.RN),
                    'keys' => ('station','sampnum','bottle')
              },
              'results' => {
                    'select' =>qq(SELECT water_samples_results_data.pipe as 'spare3', water_samples_results_data.sampnum as 'sampnum', water_samples_results_data.bottle as 'bottle', water_samples_results_data.variable_no as 'variable', water_samples_results_data.flag as 'flag', water_samples_results_data.value as 'value', water_samples_results_data.quality as 'quality', water_samples_results_data.commnt as 'commnt' FROM WATER_SAMPLES_RESULTS_DATA left outer join GW_REGDETS on GW_REGDETS.RN=WATER_SAMPLES_RESULTS_DATA.RN 
                    where GW_REGDETS.RN=WATER_SAMPLES_RESULTS_DATA.RN
                    and WATER_SAMPLES_RESULTS_DATA.SAMPNUM=WATER_SAMPLES_DATA.SAMPNUM
                    
                    ),
                    'keys' => ('station','sampnum','bottle','variable')
              },
              'elevations'=>{
                    'select' =>  qq(SELECT Gw_regdets.RN as 'station', Elevation_data.ELEVATION as 'elev', Elevation_data.PRECISION as 'elevacc' FROM GW_REGDETS left outer join ELEVATION_DATA on ELEVATION_DATA.RN=GW_REGDETS.RN 
                    where ELEVATION_DATA.RN=GW_REGDETS.RN ),
                    'keys' => 'station'
              }
              
              
            );
=skip

              'gwpurp' => { 
                    'select' => qq(SELECT Facility_roles_data.rn as 'station', Facility_roles_data.facility_role as 'purpose', Facility_roles_data.comments as 'spare5' FROM FACILITY_ROLES_DATA left outer join GW_REGDETS on GW_REGDETS.RN=FACILITY_ROLES_DATA.RN where GW_REGDETS.RN=FACILITY_ROLES_DATA.RN ),
                    'keys' => 'station'
              },

              'casing' => qq( ),
              
              'gwpipe' => qq( ),
),
              'gwtracer' => qq(),
              'gwtrace' => qq(),
              
              'hydmeas' => qq(),
              'hydrlmp' => qq(),
             
              'pumpread' => qq(),
              'pumptest' => qq(),
              
  );            
=cut              
  
  #establish a connection via ADO
  HydADO->Version(1);  
  my %connect;
  IniHash('connect.ini',\%connect);  
  Prt('-D','CONNECT.INI\n'.HashDump(\%connect)."\n");
  my $connectstring = $connect{mssqlserver}{connectstring};
  my $rawworkareaprefix = $connect{mssqlserver}{workareaprefix};
  $rawworkareaprefix =~ s{\&SCOPE}{PUBLIC};                        #manually construct the name of the work area table

  
  #ApplySpecialCases();
  #ApplyDefaults();
  #WriteTables();
  
 
  
  foreach my $table ( keys %select ){
    Prt($prtdest_scr,NowStr()."    - Assembling data for Hydstra [$table] table.\n");
    #my $hashref = $dbh->selectall_hashref( $select{$table},\@keys );
    my %data = ();
    my $aqui_count;
    if ( $table eq 'elevations') {
      # my $selct = $select{$table}{'select'};
      #my $keys = $select{$table}{'select'}{'keys'};
      #select row and then write it with HYDB.
      #my $hashref = $dbh->selectall_hashref( $select{$table}{'select'}, $select{$table}{'select'}{'keys'} );
      #while ( 
      Prt($prtdest_scr,NowStr()."    - Selecting Elevations from [$table] table.\n");
=skip
      my $row = $dbh->selectall_arrayref( qq(SELECT DISTINCT Gw_regdets.RN as 'station', Elevation_data.ELEVATION as 'elev', Elevation_data.PRECISION as 'elevacc' FROM GW_REGDETS left outer join ELEVATION_DATA on ELEVATION_DATA.RN=GW_REGDETS.RN where ELEVATION_DATA.RN=GW_REGDETS.RN and ELEVATION_DATA.MEAS_POINT = 'N'),{ Slice => {} } );
        Prt($prtdest_scr,NowStr()."    - Printing out data\n");
        foreach my $rw (@$row){
          #{ $_ = '' unless defined }
          print "Station: $rw->{station}\n";
        }
        #Prt('-P',"Data [".HashDump(\%data)."]\n");
     
      my $sth = $dbh->prepare( qq(SELECT Elevation_data.RN as 'station', Elevation_data.ELEVATION as 'elev', Elevation_data.PIPE as 'pipe', Elevation_data.PRECISION as 'elevacc', ELEVATION_DATA.MEAS_POINT as 'meas_point' FROM Elevation_data where ELEVATION_DATA.MEAS_POINT = 'N' limit 100));
      Prt($prtdest_scr,NowStr()."    - Executing.\n");
      $sth->execute();
      Prt($prtdest_scr,NowStr()."    - Done, Fetching row.\n");
      
      
      
      while ( my @row = $sth->fetchrow_array() )
      { 
        print "[\n".HashDump(\@row)."]";
      };
      Prt($prtdest_scr.'P',NowStr()."    - Done, Fetching row.\n");  
      
=cut      
      
      
      
      
      my $row = $dbh->selectall_arrayref( qq(SELECT Elevation_data.RN as 'station', Elevation_data.ELEVATION as 'elev', Elevation_data.PIPE as 'pipe', Elevation_data.PRECISION as 'elevacc', ELEVATION_DATA.MEAS_POINT as 'meas_point' 
      FROM Elevation_data 
      left outer join GW_REGDETS on ELEVATION_DATA.RN=GW_REGDETS.RN 
      where ELEVATION_DATA.RN=GW_REGDETS.RN
      and ELEVATION_DATA.MEAS_POINT = 'N' limit 100),{ Slice => {} } ); 

      Prt($prtdest_scr,NowStr()."  - Connecting to $staging_db\n");
      my $dbtmp = DBI->connect(          
          "dbi:SQLite:dbname=$sqlite3", 
          "",                          
          "",                          
          { RaiseError => 1, AutoCommit => 0},         
      ) or die $DBI::errstr;
      
      $dbtmp->do("CREATE TABLE IF NOT EXISTS HYDRLMP ($create, $primary_key)");
      " $fld $data_type, ";
      
      foreach my $rw ( @$row ){
        foreach my $key ( keys %$rw ) {
          
          
          if ($key eq 'meas_point'){
            given ($rw->{$key}){
              when (/n/)i { 
                $rw->{rlmp} = $rw->{meas_point};
                delete $rw->{meas_point};
                my $dth = $dbtmp->prepare("INSERT INTO $uctable VALUES ($vals)")
              }
              when (/r/)i { 
                $rw->{abovgnd} = $rw->{meas_point};
                delete $rw->{meas_point};
              }
            }
          }
          else{
            
          }
        
        
        
=skip      
        given ($rw->{meas_point}){
          when (/n/)i { 
            $rw->{rlmp} = $rw->{meas_point};
            delete $rw->{meas_point};
          }
          when (/r/)i { 
            $rw->{abovgnd} = $rw->{meas_point};
            delete $rw->{meas_point};
          }
          when(n) { $i = "One"; }
          when(2) { $i = "Two"; }
          when(3) { $i = "Three"; }
          default { $i = "Other"; }
        }
        rlmp
        elevacc
        ABOVGND
        RDATE as 'date' =  
        survey_source as 'comments'
        
=cut        
        
        #my $dth = $dbtmp->prepare("INSERT INTO $uctable VALUES ($vals)");
        #$dbtmp->do("CREATE TABLE IF NOT EXISTS $uctable ($create, $primary_key)");
        
        
        print "sation: [$rw->{station}], Elev: [$rw->{elev}], Elevacc: [$rw->{elevacc} ]\n";
      
      
      }
      Prt($prtdest_scr.'P',NowStr()."    - Done, Fetching row.\n");  
      
      
      
      
      #                    ){
         #print "@$row\n";
        #%data = %{$hashref};
      #}
=skip
      my $data_key;
      my $selct = $select{$table}{'select'};
      my $keys = $select{$table}{'select'}{'keys'};
      my $sth  = $dbh->prepare($selct, $keys);
      $sth->execute();

      while ( my @arr = $sth->fetchrow_array() ) 
      {
            Prt('-P',"arr [".HashDump(\@arr)."]\n");
      }
=cut
      
      
      
      
      #foreach my $key (keys $select{$table}{'select'}{'keys'}){
      #  $data_key .= 
      
      #}
      
      #$aqui_count = keys %data;
      
      #ApplyDefaults();
      #WriteTables();
      
      #foreach my $key ( keys %select ){
      #  $data{$key};
      #} 
    }
    else{
      #Prt($prtdest_scr,NowStr()."    - Selecting all hashref.\n");
      #my $hashref = $dbh->selectall_hashref( $select{$table},['station','depthfrom']);
      #%data = %{$hashref};
      #$aqui_count = keys %data;
    }
    Prt($prtdest_scr,NowStr()."     - Done, printing to log.\n");
    
    Prt('-L',"TABLE [$table], key count [$aqui_count]\n");
    #Prt('-L',"HashDump [".HashDump(\%data)."]\n");
    
=skip    
    given ($table) {
    
          when (/aquifer/i) {
            $deletecommand = qq(DELETE a FROM [hydstra].[dbo].[$table] a INNER JOIN [hydstra].[dbo].[$workareaprefix$table] b ON a.STATION=b.STATION AND a.DEPTHFROM=b.DEPTHFROM);
          }
          when (/areasmt/i) {
            $deletecommand = qq(DELETE a FROM [hydstra].[dbo].[$table] a INNER JOIN [hydstra].[dbo].[$workareaprefix$table] b ON a.STATION=b.STATION AND a.ASSESSTYPE=b.ASSESSTYPE AND a.DATE=b.DATE);
          }
          when (/aresite|gwhole|gwpipe|site/i) {    #find the Site ID in the work area copy of ARESITE
            $deletecommand = qq(DELETE a FROM [hydstra].[dbo].[$table] a INNER JOIN [hydstra].[dbo].[$workareaprefix$table] b ON a.STATION=b.STATION);
          }
          when (/artpress/i) {
             $deletecommand = qq(DELETE a FROM [hydstra].[dbo].[$table] a INNER JOIN [hydstra].[dbo].[$workareaprefix$table] b ON a.STATION=b.STATION AND a.DATE=b.DATE AND a.TIME=b.TIME);
          }
          when (/artsumm|hydrlmp/i) {
            $deletecommand = qq(DELETE a FROM [hydstra].[dbo].[$table] a INNER JOIN [hydstra].[dbo].[$workareaprefix$table] b ON a.STATION=b.STATION AND a.DATE=b.DATE);
          }
          when (/casing/i) {
             $deletecommand = qq(DELETE a FROM [hydstra].[dbo].[$table] a INNER JOIN [hydstra].[dbo].[$workareaprefix$table] b ON a.STATION=b.STATION AND a.DEPTHFROM=b.DEPTHFROM AND a.OUTDIAM=b.OUTDIAM);
          }
          when (/company/i) {
            $deletecommand = qq(DELETE a FROM [hydstra].[dbo].[$table] a INNER JOIN [hydstra].[dbo].[$workareaprefix$table] b ON a.COMPANYID=b.COMPANYID);
          }
          when (/gwpurp/i) {
            $deletecommand = qq(DELETE a FROM [hydstra].[dbo].[$table] a INNER JOIN [hydstra].[dbo].[$workareaprefix$table] b ON a.STATION=b.STATION AND a.DATE=b.DATE AND a.PURPOSE=b.PURPOSE);
          }
          when (/hydmeas/i) {
            $deletecommand = qq(DELETE a FROM [hydstra].[dbo].[$table] a INNER JOIN [hydstra].[dbo].[$workareaprefix$table] b ON a.STATION=b.STATION AND a.DATE=b.DATE AND a.TIME=b.TIME AND a.VARIABLE=b.VARIABLE);
          }
          when (/pumpread/i) {
            $deletecommand = qq(DELETE a FROM [hydstra].[dbo].[$table] a INNER JOIN [hydstra].[dbo].[$workareaprefix$table] b ON a.STATION=b.STATION AND a.TESTDATE=b.TESTDATE AND a.STARTTIME=b.STARTTIME AND a.CUMDURAT=b.CUMDURAT);
          }
          when (/pumptest/i) {
            $deletecommand = qq(DELETE a FROM [hydstra].[dbo].[$table] a INNER JOIN [hydstra].[dbo].[$workareaprefix$table] b ON a.STATION=b.STATION AND a.TESTDATE=b.TESTDATE AND a.STARTTIME=b.STARTTIME);
          }
          when (/results/i) {
            $deletecommand = qq(DELETE a FROM [hydstra].[dbo].[$table] a INNER JOIN [hydstra].[dbo].[$workareaprefix$table] b ON a.STATION=b.STATION AND a.SAMPNUM=b.SAMPNUM AND a.BOTTLE=b.BOTTLE AND a.VARIABLE=b.VARIABLE);
          }
          when (/samples/i) {
            $deletecommand = qq(DELETE a FROM [hydstra].[dbo].[$table] a INNER JOIN [hydstra].[dbo].[$workareaprefix$table] b ON a.STATION=b.STATION AND a.SAMPNUM=b.SAMPNUM AND a.BOTTLE=b.BOTTLE);
          }
          default {    #otherwise use a 'hit file' delete
            Prt( '-W', "WARNING: Deleting records from $table using HYDBUTIL DELETE may not be as efficient as a direct SQL DELETE\n" );
            $deletecommand = qq(HYDBUTIL DELETE $table [PUB.$workarea$workareatype]$table "$stagereportfile+" /FASTMODE);
          }
        }
        
      if ($deletecommand =~ m{^delete}i){
        $db->ExecSQL($deletecommand);
      }
      
      PrintAndRun('-S','HYDBUTIL APPEND '.uc($table).' '.uc($work_table).' TABLE NO hydbutil.report.txt');
    
    #append new records to the archive
    Prt( $prtlog, NowStr() . "           - Appending new records from [PUB.$workarea$workareatype]$table\n" );
    if ( !PrintAndRun( $prtlog, qq(HYDBUTIL APPEND $table [PUB.$workarea$workareatype]$table TABLE NO "$stagereportfile+" /FASTMODE) ) ) {
      Prt( $prtlog, NowStr() . "             - success\n" );
    }
    else {
      $return = 0;
      Prt( $prtlog, NowStr() . "             - failed\n" );
      push( @{ $status{stages}{ArchiveWorkArea}{errors} }, "Could not append new data from [PUB.$workarea$workareatype]$table" );
    }
=cut 
    
    
  
 }
   
  
  
  
  #my $hashref = $dbh->selectall_hashref( $site_select,'station');
  

=skip
  my %hydb;
  my $workarea = uc( $table.$db_date );
  my $data_key_count = 0;
  foreach my $site ( keys %data ){
    
    $hydb{$table} = HyDB->new($table,$workarea,{allowdupes => 0, printdest => '-L'});  
  
    foreach my $default_field ( keys %defaults ){
      $data{$site}{$default_field} = $defaults{$default_field};
    }
    
    #Prt('-L',"ALL [".HashDump($hashref)."]\n");
    #my $station = $data{$table}{$key}{'station'};
    
    my $keycount = keys %data;
    $data_key_count++;
    if ($data_key_count%10==0) {	
      Prt($prtdest_scr,NowStr()."         - Writing table [$table]: record #$data_key_count/$keycount   \r");
    }
    
    else{
      Prt($prtdest_debug,NowStr()."         - writing key [$sitey]\n");
      $hydb{$table}->sethash(\%{$data{$table}{$site}});
      $hydb{$table}->write;
      $hydb{$table}->clear; 
      #$workareas{$workarea}{$table}++; 
    }
    $hydb{$table}->close();
  }
=cut  
  
=skip
        select SITE.SPARE1 as 'Tenure Holder Bore ID',
        RESULTS.VARIABLE as 'Parameter', RESULTS.VALUE as 'Value',
        SAMPLES.SRCSAMP as 'Sample Source'
        from SITE
        left outer join RESULTS on RESULTS.STATION=SITE.STATION
        left outer join SAMPLES on SAMPLES.STATION=SITE.STATION
        where SITE.STATION=RESULTS.STATION
        and SAMPLES.STATION=RESULTS.STATION 
        and RESULTS.VARIABLE=5401
        or RESULTS.VARIABLE=5402
        order by SITE.SPARE1
  =cut


  
  
  foreach my $hytable ( keys %mappings ){
    my $field_count = keys %{$mappings{$hytable}{'fields'}};
    
    my $counter = 0;
    my $select = 'SELECT';
    my @keys;
    
    
    #Create SELECT statement for table
    foreach my $gwdb_field ( keys %{$mappings{$hytable}{'fields'} } ){
      $counter++;
      $select .= ' '.$mappings{$hytable}{'tables'}.'.'uc($gwdb_field).' as '.$mappings{$hytable}{'fields'}{$gwdb_field}.',';
      ($field_count == $counter) ? $select =~ s{\,$}{)}:next;
    }
    $select .= "from $table";
    
    
    #Create KEYS array for table
    foreach my $key ( keys %{$mappings{$hytable}{'keys'} } ){
      push (@keys, $key);
    }
    
    #$selects{$table} = $select;
    #Prepare the select statement, and then get the hashref
    my $statement = $dbh->prepare($select);
    my $hashref = $dbh->selectall_hashref($statement,\@keys);
    
    
  }
  
  
  
  
  site.station,gwhole.station,gwpipe.station,pumptest.station,aresite.station,company.station',
  
  my $gwhole_select = qq( )
  
  my %selects = (
                  'site' -> $site_select,
 ',
                  
  
  
  );
=cut  
  
  #my $statement = "SELECT * FROM AQUIFER_DATA LIMIT 10";
  #my $statement = "SELECT * FROM AQUIFER_DATA INNER JOIN GW_REGDETS ON GW_REGDETS.RN = AQUIFER_DATA.RN LIMIT 10";
  #my $statement = "SELECT TOP, BOTTOM, FORM_DESC, CONDITION, CONTR, FLOW, QUALITY, YIELD, SWL, RDATE FROM AQUIFER_DATA INNER JOIN GW_REGDETS ON AQUIFER_DATA.RN = GW_REGDETS.RN LIMIT 10";
  
  
  #my $hashref = $stp->selectall_hashref();
  #my $hashref = $dbh->selectall_hashref($statement,'Rn');
  
  ##Prt('-L',"ALL [".HashDump($hashref)."]\n");
 
=skip
  while (@row = $stp->fetchrow_array()){
    #Prt('-PL',"row [".HashDump(\@row)."]\n");
  }
 =cut  
  Prt('-PL',"-------END DATA-------\n");
  
 # $stp->finish();      
  
  $dbh->disconnect();
  
  
  
  
  #$dbh->do("INSERT INTO Cars VALUES(1,'Audi',52642)");
  #65054|1|UC|22.5|23.5|||||||ELLIOTT
  #my $sth = $dbh->prepare( "PRAGMA table_info(Aquifer_data)" );  
  #$sth->execute();
  
 # my $sth = $dbh->column_info(undef, "main", "Cars");
#my @ary = $sth->fetchrow_array();
      
#print join(" ", @ary), "\n";                   
 
  #"RN"|"REC"|"CONDITION"|"TOP"|"BOTTOM"|"CONTR"|"FLOW"|"QUALITY"|"YIELD"|"SWL"|"RDATE"|"FORM_DESC"
  #$dbh->do("INSERT INTO Aquifer VALUES(65054,1,UC,22.5,23.5,,,,,,,ELLIOTT)");
  #$dbh->do("INSERT INTO Aquifer_data VALUES(65054,1,'UC',22.5,23.5,'','','','','','','ELLIOT')");
   #     my $hed = $dbh->fetchrow_hashref();
 
  
  
  
=skip
  my @row;
  while (@row = $sth->fetchrow_array()) {
    #Prt('-P',"Fetchrow_array [".HashDump(\@row)."]\n");
  }  
=cut
  
=skip  
  #Prt('-P',"\nHashDump Tables [".HashDump(\@tables)."]\n\n");
  foreach my $table (@tables){
    my $stp = $dbh->prepare( "SELECT * FROM $table LIMIT 500" );  
    $stp->execute();
          
    #my ($ids, $name, $price) = $stp->fetchrow();
    my @dat = $stp->fetchrow();
    #my %hashref = $stp->selectall_hashref();
    ##Prt('-P',"ids [$id] name [$name] price [$price]\n");
    ##Prt('-P',"Aquifer data [\n".HashDump(\@dat)."]\n");
    ##Prt('-P',"Aquifer data [\n".HashDump(\%hashref)."]\n");
    my @row;
    #Prt('-L',"\n-------[$table] START-------\n");
    while (@row = $stp->fetchrow_array()){
      #Prt('-L',"row [".HashDump(\@row)."]\n");
    }
    #Prt('-L',"-------[$table] END-------\n");
    
    $stp->finish();
  }
  $dbh->disconnect();
  $dbh->do("DROP TABLE IF EXISTS Cars");
  $dbh->do("CREATE TABLE Cars(Id INT PRIMARY KEY, Name TEXT, Price INT)");
  $dbh->do("INSERT INTO Cars VALUES(2,'Mercedes',57127)");
  $dbh->do("INSERT INTO Cars VALUES(3,'Skoda',9000)");
  $dbh->do("INSERT INTO Cars VALUES(4,'Volvo',29000)");
  $dbh->do("INSERT INTO Cars VALUES(5,'Bentley',350000)");
  $dbh->do("INSERT INTO Cars VALUES(6,'Citroen',21000)");
  $dbh->do("INSERT INTO Cars VALUES(7,'Hummer',41400)");
  $dbh->do("INSERT INTO Cars VALUES(8,'Volkswagen',21600)");

=cut
=skip

 my $htm_template = 'hybootstrap.htm';
   
 my $bootdiv = HyBootstrap->New($htm_template); 

 $bootdiv->Div('div class="row"','','');
    $bootdiv->Div('div class="span7"','','');
      $bootdiv->Div('h2','Sigra telemetry import report','h2');
    $bootdiv->Div('','','div');
    $bootdiv->Div('div class="span2 offset3"','','');
      $bootdiv->Div('img src="img/logo-arrow-energy.png"','','img');
    $bootdiv->Div('','','div');
  $bootdiv->Div('','','div');
  
  $bootdiv->Div('p class="muted"',"Generated by ".FileNameExt($0)." @ ".NowStr()."",'p');
  
  my $error_count = $report{errors}//0;
  my $group_count = keys %{ $report{'error'}{'groups'} };
  my $grp_rep_string;
  
  foreach my $group ( keys %{ $report{'error'}{'groups'} }){
    $grp_rep_string .= " $group [$report{'error'}{'groups'}{$group}{'stn_count'}]";
  } 
  
  #$bootdiv->Div('p',"Site count for $grp_rep_string",'p');
  
  if ( $error_count > 0){
    $bootdiv->Div('div class="alert alert-error"','There are errors in the import. Please address each error listed below.','div');
  }
  elsif( $error_count == 0){
    $bootdiv->Div('div class="alert alert-success"','Successful import.','div');
    #$bootdiv->Div('div class="well well-small"','Successful import.','div');
  }
  
  
  
  #$bootdiv->Div('div class="well"',$report{'response'}{2},'div'); 
  
  
  #$report{'errors'}{'ts'}{$site}{'err_msg'} = $msg;
  
  $bootdiv->Div('table class="table table-hover table-condensed"','','');
  #$bootdiv->Div('caption','Table output','caption');
  $bootdiv->Div('thead','','');
  $bootdiv->Div('tr','','');
  $bootdiv->Div('th','','');
    $bootdiv->Div('i style="color:yellow" class="icon-info-sign"','','i');
  $bootdiv->Div('','','th');
  $bootdiv->Div('th','','');
    $bootdiv->Div('i class="icon-map-marker"',' ','i');
  $bootdiv->Div('',' Site','th');
  $bootdiv->Div('th','','');
    $bootdiv->Div('i class="icon-tint"',' ','i');
  $bootdiv->Div('',' Variable','th');
  $bootdiv->Div('th','','');
    $bootdiv->Div('i class="icon-align-justify"',' ','i');
  $bootdiv->Div('','Line Count','th');
  $bootdiv->Div('th','','');
    $bootdiv->Div('i class="icon-exclamation-sign"',' ','i');
  $bootdiv->Div('','Error','th');
  $bootdiv->Div('th','','');
    $bootdiv->Div('i class="icon-user"',' ','i');
  $bootdiv->Div('','User Action','th');
  $bootdiv->Div('','','tr');
  $bootdiv->Div('','','thead');
  $bootdiv->Div('tbody','','');
  
foreach my $error (keys %{$report{error}}) {
    #$report{'error'}{'ts'}{$site}{'err_msg'} = $msg;
    #$report{'error'}{'site'}{$site}{'err_msg'}
    foreach my $stn ( keys %{$report{'error'}{$error} } ){
      foreach my $var ( keys %{$report{'error'}{$error}{$stn} } ){
    
        #@row = ();
        #push(@row,$report->Text('p',$file));
        #push(@row,$report->Text('p',$report{error}{$error}{linecount}));
        my $errors='';
        if ( defined($report{'error'}{$error}{$stn}{$var}{'err_msg'})) {
          $bootdiv->Div('tr class="error"','','');
          my $error = $report{'error'}{$error}{$stn}{$var}{'err_msg'};
            #$errors .= $error.'<br>';
            #$bootdiv->Div('h1',$error,'h1');
            $bootdiv->Div('td','','');
              #$bootdiv->Div('span class="badge badge-important"','','');
                $bootdiv->Div('i class="icon-warning-sign icon-red"','','i');
              #$bootdiv->Div('','','span');
            $bootdiv->Div('','','td');
            $bootdiv->Div('td','','td');
            $bootdiv->Div('td','','td');
            $bootdiv->Div('td','-','td');
            $bootdiv->Div('td class="text-error"',$error,'td');
            $bootdiv->Div('td','','');
            $bootdiv->Div('button class="btn btn-success"','Accept','button');
            $bootdiv->Div('button class="btn btn-danger"','Reject','button');
            $bootdiv->Div('','','td');
        }
        else {
        #$errors = '-';
        $bootdiv->Div('tr','','');
          $bootdiv->Div('td','','');
            $bootdiv->Div('i class="icon-ok icon-green"','','i');
          $bootdiv->Div('','','td');
          $bootdiv->Div('td',$stn,'td');
          $bootdiv->Div('td',$var,'td');
          $bootdiv->Div('td',$report{'error'}{$error}{$stn}{$var}{'line_count'}//'','td');
          $bootdiv->Div('td class="text-success"','All good','td');
          $bootdiv->Div('td','No action required','td');
        }
      }
    }
    
    #push(@row,$report->Text('p',$errors));
    #$outtable->Row('next',\@row);
    #$outtable->Row('last',{'valign'=>'top'});
    
    $bootdiv->Div('','','tr');
  
  }
  
  $bootdiv->Div('','','tbody');
  $bootdiv->Div('','','table');
  $bootdiv->HTML;
  
  my %bootdiv = %{$bootdiv};
  #Prt('-L', "BOOTDIV from resmon3\n".HashDump(\%bootdiv)."\n]]\n");
  #my $reportfile = JunkFile('htm');
  OpenFile(*HTML,$reportfile,">");
  #Prt(*HTML,$bootdiv->Page);
  
  close(HTML);
=cut

  #Prt('-S',"Got through to the end of the file\n");
  #Prt($prtdest_scr.'L',NowStr()." - Finnished \n");
  my $end_time = NowRel();
  my $total_time = $end_time - $start_time;
  Prt('-RL',"Dump of report [".HashDump(\%report)."]\n");
  Prt($prtdest_scr.'RL',NowStr()." - Finnished $prog \n");
  
  
}
