#!/usr/bin/perl -w
use FindBin qw($Bin);
use JSON;
use constant DB_DIR => $Bin.'/tables/'; 

mkdir DB_DIR if (! -d DB_DIR );

 my %primary_keys = (
          aquifer                  => { rn => 1, rec => 1, top => 1 },
          casing                   => { rn => 1, rec => 1, pipe => 1, top => 1, out_diameter => 1 , rdate => 1 },
          elevations                => { rn => 1, pipe => 1, rdate => 1 },
          facility_roles          => { rn => 1 , facility_role => 1 },
          water_quality_field          => { rn => 1, pipe => 1, rdate => 1 },
          multiple_conductivity    => { rn => 1, pipe => 1, rdate => 1, depth => 1 },
          registration                    => { rn => 1 },
          water_quality_variables              => { variable_no => 1 },
          strata_log               => { rn => 1, rec => 1, top => 1 },
          stratigraphy             => { rn => 1, rec => 1, top => 1 },
          water_analysis           => { rn => 1, rec => 1, pipe => 1, rdate => 1 },
          water_levels                  => { rn => 1, pipe => 1, rdate => 1 },
          special_analysis_results    => { rn => 1, pipe => 1, sampnum => 1, bottle => 1, variable_no => 1 },
          special_analysis_sample            => { rn => 1, pipe => 1, sampnum => 1, bottle => 1  },
  );


my  %data_type_mapping = (
                  aquifer =>      [#AQUIFER.txt
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
                  casing  =>     [#CASINGS.txt
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
                  elevations =>   [#ELEVATIONS.txt
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
                  facility_roles=>[#FACILITY_ROLES.txt
                                  #"RN"|"FACILITY_ROLE"|"COMMENTS"
                                  #65912|WS| 
                                  {rn => 'numeric'},
                                  {facility_role => 'text'},
                                  {comments => 'text'}
                                 ],
                  water_quality_field => [ #FIELD_WATER_QUALITY.txt
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
                  multiple_conductivity => [#MULTIPLE_COND.txt
                                  #"RN"|"PIPE"|"RDATE"|"DEPTH"|"MEASUREMENT"|"TEMP"
                                  #13600158|A|11/04/1990|-3|39400| 
                                  {rn => 'numeric'}, 
                                  {pipe => 'text'},
                                  {rdate => 'text'},  
                                  {depth => 'real'}, 
                                  {measurement => 'real'},  
                                  {temp => 'real'}  
                                 ],
                  registration => [#REGISTRATIONS.txt or #registration.TXT
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
                                  {data_owner => 'text'},
                                  {bore_line_code => 'text'},
                                  {driller_licence_number => 'text'},
                                  {log_received_date => 'text'}
                                ],
                                
                  water_quality_variables =>   [#strata_log.txt
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
                  strata_log =>   [#strata_log.txt
                                  #"RN"|"REC"|"DESCR"|"TOP"|"BOTTOM"
                                  #95987|1|TOPSOIL|0|.3
                                  #rn => 'site.station,gwhole.station,lithdril.station,', 
                                  {rn => 'numeric'}, 
                                  {rec => 'numeric'}, 
                                  {descr => 'text'}, 
                                  {top => 'numeric'}, 
                                  {bottom => 'numeric'}
                                  ],
                  stratigraphy =>  [#stratigraphy.txt
                                  #"RN"|"REC"|"DATA_OWNER"|"FORM_DESC"|"TOP"|"BOTTOM"
                                  #13700092|1|DNR|ELLIOTT FORMATION|0|18.9
                                  {rn => 'numeric'}, 
                                  {rec => 'text'},  
                                  {data_owner => 'text'},  
                                  {form_desc => 'text'},  
                                  {top => 'real'}, 
                                  {bottom => 'real'}
                                 ],
                               
                  water_analysis =>[#water_analysis.txt
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
                  special_analysis_results =>    [#WQ_SAMPLES_RESULTS_DATA.txt
                                  #"RN"|"PIPE"|"SAMPNUM"|"BOTTLE"|"VARIABLE_NO"|"FLAG"|"VALUE"|"QUALITY"|"COMMNT"
                                  #12600532|A|182289|B|2741.2|<|.004|| 
                                  #I believe this results table needs to have a key that is in accordance with the 
                                  #special_analysis_sample file.
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
                  special_analysis_sample =>    [#WQ_SAMPLES_DATA.txt
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
                                  {commnt => 'text'}
                                ]
  );
  
 my %mappings = (
                  aquifer =>      {#AQUIFER.txt
                                  #"RN"|"REC"|"CONDITION"|"TOP"|"BOTTOM"|"CONTR"|"FLOW"|"QUALITY"|"YIELD"|"SWL"|"RDATE"|"FORM_DESC"
                                  #65054|1|UC|22.5|23.5|||||||ELLIOTT
                                  rn => 'aquifer.station,gwhole.station',
                                  rec => 'aquifer.rec', #not used.
                                  top => 'aquifer.depthfrom',
                                  bottom => 'aquifer.depthto',
                                  form_desc => 'aquifer.name,aquifer.comment',
                                  condition => 'aquifer.spare1', #DESCRIPTION CODE, Porous Rocks: Unconsolidated UC,Consolidated PS,Semi-Consolidated SC,Fractured Rocks:Fractured FR,Vesicular VS,Cavernous CV,Weathered Zone WZ
                                  contr => 'aquifer.spare5',
                                  flow => 'aquifer.spare4',
                                  quality => '',
                                  yield => 'aquifer.yield',
                                  swl => 'aquifer.swlvalue',
                                  rdate => 'aquifer.spare3',
                                },                         
                  casing  =>     {#CASINGS.txt
                                  #"RN"|"PIPE"|"RDATE"|"REC"|"MATERIAL_DESC"|"MATERIAL_SIZE"|"SIZE_DESC"|"OUT_DIAMETER"|"TOP"|"BOTTOM"
                                  #65174|A|23/11/1986|1|PLAS|5|WT|110|0|19.8 
                                  #all casings are depth below ground level
                                  rn => 'casing.station,gwhole.station,gwpipe.station',
                                  pipe => 'casing.pipe,gwpipe.pipe', 
                                  rdate => 'casing.statusdate', 
                                  rec => 'ignore.rec', #not used.
                                  material_desc => 'casing.casetype', 
                                  material_size => 'casing.aperture', 
                                  size_desc => 'casing.size_desc', #dummy placeholder to be processed in special cases 
                                  out_diameter => 'casing.outdiam', 
                                  top => 'casing.depthfrom', 
                                  bottom => 'casing.depthto', 
                                 },
                  elevations =>   {#ELEVATIONS.txt
                                  #"RN"|"PIPE"|"RDATE"|"MEAS_POINT"|"PRECISION"|"DATUM"|"ELEVATION"|"SURVEY_SOURCE"
                                  #22013|X|01/01/1923|N|EST|STD|16.7|  
                                  #A B C for pipes = R for reference point, N then is will be pipe X which is ground surface
                                  rn => 'elevations.station,gwhole.station,gwpipe.station', 
                                  pipe => 'elevations.pipe,gwpipe.pipe', 
                                  rdate => 'elevations.rdate', 
                                  meas_point => 'elevations.meas_point', 
                                  precision => 'elevations.precision', 
                                  datum => 'elevations.datum', 
                                  elevation => 'elevations.elevation', 
                                  survey_source => 'elevations.survey_source', 
                                 },                             
                  facility_roles=>{#FACILITY_ROLES.txt
                                  #"RN"|"FACILITY_ROLE"|"COMMENTS"
                                  #65912|WS| 
                                  rn => 'gwhole.station,gwpurp.station',
                                  facility_role => 'gwpurp.purpose',
                                  comments => 'gwpurp.spare5',
                                 },
                  water_quality_field => { #FIELD_WATER_QUALITY.txt
                                    #"RN"|"PIPE"|"RDATE"|"SAMP_METHOD"|"SOURCE"|"DEPTH"|"CONDUCT"|"DO2"|"EH"|"NO3"|"PH"|"TEMP"|"ALKALINITY"
                                    #40044|A|22/12/1976||GB||183||||||
                                   
                                  rn => 'samples.station,results.station',
                                  pipe => 'samples.spare3', 
                                  rdate => 'samples.date,samples.sampnum,results.sampnum', 
                                  samp_method => 'samples.samp_method',
                                  source => 'samples.source',
                                  depth => 'analysis.630', #Bore water level
                                  conduct => 'analysis.821', 
                                  do2 => 'analysis.2351',
                                  eh => 'analysis.2102', #EH = REDOX
                                  no3 => 'analysis.2331', 
                                  ph => 'analysis.2100',
                                  temp => 'analysis.450',
                                  alkalinity => 'analysis.2123', 
                                 },
                  multiple_conductivity => {#MULTIPLE_COND.txt
                                    #"RN"|"PIPE"|"RDATE"|"DEPTH"|"MEASUREMENT"|"TEMP"
                                    #13600158|A|11/04/1990|-3|39400| 
                                  rn => 'gwhole.station,gwpipe.station,gwtracer.station,gwtrace.station', 
                                  pipe => 'gwtracer.pipe,gwtrace.pipe,gwpipe.pipe',
                                  rdate => 'gwtracer.date,gwtrace.date',  
                                  depth => 'gwtracer.depth', 
                                  measurement => 'gwtracer.821',  
                                  temp => 'gwtracer.450',  
                                 },
                  registration => {#REGISTRATIONS.txt or #registration.TXT
                                    #"RN"|"FACILITY_TYPE"|"OFFICE"|"SHIRE_CODE"|"PARISH"|"RN_REPLACES"|"DO_FILE"|"RO_FILE"|"HO_FILE"|"FACILITY_STATUS"|"DRILLED_DATE"|"DRILLER_NAME"|"DRILLING_COMP"|"BASIN"|"METHOD_CONST"|"SUB_AREA"|"LOT"|"PLAN"|"DESCRIPTION"|"COUNTY"|"PROPERTY_NAME"|"LAT"|"LNG"|"EASTING"|"NORTHING"|"ZONE"|"ACCURACY"|"GPS_ACCURACY"|"GIS_LAT"|"GIS_LNG"|"CHECKED"|"MAP_SCALE"|"MAP_SERIES"|"MAP_NO"|"PROG_SECT"|"EQUIPMENT"|"ORIG_NAME_NO"|"POLYGON"|"CONFIDENTIAL"|"DATA_OWNER"|"BORE_LINE_CODE"|"DRILLER_LICENCE_NUMBER"|"LOG_RECEIVED_DATE"|"SHAPE"
                                    #19|SF|BBG|6630|1039||||||01/01/1908|||1363|||||P75V|FITZROY||||||56||||||104|M|9245|||O/S PROCLAIMED AREA|||DNR||||
                                  rn => 'site.station,gwhole.station,gwpipe.station,pumptest.station,aresite.station',
                                  facility_type => 'site.stntype',
                                  office => 'ignore.office',
                                  shire_code => 'ignore.shire_code',
                                  parish => 'ignore.parish',
                                  county => 'ignore.county',
                                  rn_replaces => 'ignore.rn_replaces',
                                  do_file => 'ignore.do_file',
                                  ro_file => 'ignore.ro_file',
                                  ho_file => 'ignore.ho_file',
                                  facility_status => 'gwhole.conststat',
                                  drilled_date => 'gwhole.startdate,gwhole.enddate,pumptest.testdate', #do I need to have pumptest??
                                  driller_name => 'ignore.surname',
                                  drilling_comp => 'ignore.name',
                                  basin => 'site.category1',
                                  method_const => 'gwhole.constmeth',
                                  sub_area => 'ignore.sub_area',
                                  lot => 'aresite.lot',
                                  plan => 'aresite.plan',
                                  description => 'site.comment',
                                  property_name => 'aresite.property',
                                  lat => 'site.latitude',
                                  lng => 'site.longitude',
                                  easting => 'site.easting',
                                  northing => 'site.northing',
                                  zone => 'site.zone',
                                  accuracy => 'site.posacc',
                                  gps_accuracy => 'site.posacc',
                                  gis_lat => 'site.latitude',
                                  gis_lng => 'site.longitude',
                                  checked => 'ignore.checked ',
                                  map_scale => 'ignore.map_scale',
                                  map_series => 'ignore.map_series',
                                  map_no => 'site.mapname',
                                  prog_sect => 'ignore.prog_sect',
                                  equipment => 'pumptest.equipment',
                                  orig_name_no => 'ignore.orig_name_no',
                                  polygon => 'ignore.spare1',
                                  confidential => 'ignore.confidential',
                                  data_owner => 'site.owner',
                                  bore_line_code => 'ignore.bore_line_code',
                                  driller_licence_number => '',
                                  log_received_date => 'ignore.log_received_date',
                                  shape => 'ignore.shape',
                                },
                                
                  water_quality_variables =>   {#strata_log.txt
                                  #"VARIABLE_NO"|"NAME"|"UNIT_CODE"|"UNITS"|"SHORTNAME"|"COMMNT"|"MINIMUM"|"MAXIMUM"|"ACCURACY"|"PRECISION"|"REPORTFORM"|"ELECCHARGE"|"MOLAREQUIV"|"CASEREF"
                                  #766.24|Total Runoff  (event)|MM|mm|Total Runoff|event based|0|0|0|.00001|LEVEL||0|
 
                                  'variable_no'=> 'variable.varnum',
                                  'name'=> 'variable.varnam',
                                  'unit_code'=> 'variable.unitcode',
                                  'units'=> 'variable.varunit',
                                  'shortname'=> 'variable.shortname',
                                  'commnt'=> 'variable.comment',
                                  'minimum'=> 'variable.minimum',
                                  'maximum'=> 'variable.maximum',
                                  'accuracy'=> 'variable.accuracy',
                                  'precision'=> 'variable.precision',
                                  'reportform'=> 'variable.reportform',
                                  'eleccharge'=> 'variable.eleccharge',
                                  'molarequiv'=> 'variable.molarequiv',
                                  'caseref' => 'variable.casref',
                                  },
                  strata_log =>   {#strata_log.txt
                                    #"RN"|"REC"|"DESCR"|"TOP"|"BOTTOM"
                                    #95987|1|TOPSOIL|0|.3
                                  #rn => 'site.station,gwhole.station,lithdril.station,', 
                                  rn => 'lithdril.station', 
                                  rec => 'lithdril.rec', 
                                  descr => 'lithdril.comment', 
                                  top => 'lithdril.depthfrom', 
                                  bottom => 'lithdril.depthto',
                                  },
                  stratigraphy =>  {#stratigraphy.txt
                                    #"RN"|"REC"|"DATA_OWNER"|"FORM_DESC"|"TOP"|"BOTTOM"
                                    #13700092|1|DNR|ELLIOTT FORMATION|0|18.9
                                  rn => 'lithstra.station', 
                                  rec => 'lithstra.rec',  
                                  data_owner => 'lithstra.person',  
                                  form_desc => 'lithstra.comments',  
                                  top => 'lithstra.depthfrom', 
                                  bottom => 'lithstra.depthto',
                                 },
                  water_analysis =>{#water_analysis.txt
                                    #"RN"|"PIPE"|"RDATE"|"REC"|"ANALYST"|"ANALYSIS_NO"|"SAMP_METHOD"|"SOURCE"|"PRESMETH1"|"COLLSAMP"|"PROJECT1"|"DEPTH"|"CONDUCT"|"PH"|"COLOUR"|"COLOUR_IND"|"TURB"|"TURB_IND"|"SIO2"|"SIO2_IND"|"HARD"|"ALK"|"ALK_IND"|"FIG_MERIT"|"NA_ADS_RATIO"|"RES_ALK"|"TOTAL_IONS"|"TOTAL_SOLIDS"|"NA"|"NA_IND"|"K"|"K_IND"|"CA"|"CA_IND"|"MG"|"MG_IND"|"FE"|"MN"|"FE_IND"|"MN_IND"|"HCO3"|"HCO3_IND"|"CO3"|"CO3_IND"|"CL"|"CL_IND"|"F"|"F_IND"|"NO3"|"NO3_IND"|"SO4"|"SO4_IND"|"ZN"|"ZN_IND"|"AL"|"AL_IND"|"B"|"B_IND"|"CU"|"CU_IND"|"PO4"|"PO4_IND"|"BR"|"BR_IND"|"I"|"I_IND"
                                    #19|A|11/09/1986|1|GCL|116325||GB||||0|2050|8|||||32||860|325||4.7|1.3|0|1290|1120|85||1.8||180||100||.08|3.1|||390||3.2||510||.2||.9||18||||||||||0||||| 
                                    rn => 'results.station,samples.station',
                                    pipe => 'samples.spare3', 
                                    rdate => 'samples.date,samples.sampnum,results.sampnum', 
                                    rec => 'analysis.rec',
                                    analyst => 'samples.labref',
                                    analysis_no => 'samples.labsampref,samples.sampnum,results.sampnum',
                                    samp_method => 'samples.collmeth',
                                    source => 'samples.source',
                                    presmeth1 => 'samples.presmeth1',
                                    collsamp => 'samples.collsamp',
                                    project1 => 'samples.project1',
                                    depth => 'analysis.110',
                                    conduct => 'analysis.821', 
                                    ph => 'analysis.2100',
                                    colour => 'analysis.2051',
                                    colour_ind => 'analysis.2051_flag',
                                    turb => 'analysis.810',
                                    turb_ind => 'analysis.810_flag',
                                    sio2 => 'analysis.2761',
                                    sio2_ind => 'analysis.2761_flag',
                                    hard => 'analysis.2132',
                                    alk => 'analysis.2123',
                                    alk_ind => 'analysis.2123_flag',
                                    fig_merit => 'analysis.1999',
                                    na_ads_ratio => 'analysis.2143',
                                    res_alk => 'analysis.2115',
                                    total_ions => 'analysis.2170',
                                    total_solids => 'analysis.2169',
                                    na => 'analysis.2391',
                                    na_ind => 'analysis.2391_flag',
                                    k => 'analysis.2381',
                                    k_ind => 'analysis.2381_flag',
                                    ca => 'analysis.2301',
                                    ca_ind => 'analysis.2301_flag',
                                    mg => 'analysis.2321',
                                    mg_ind => 'analysis.2321_flag',
                                    fe => 'analysis.2681',
                                    mn => 'analysis.2711',
                                    fe_ind => 'analysis.2681_flag',
                                    mn_ind => 'analysis.2711_flag',
                                    hco3 => 'analysis.2125',
                                    hco3_ind => 'analysis.2125_flag',
                                    co3 => 'analysis.2124',
                                    co3_ind => 'analysis.2124_flag',
                                    cl => 'analysis.2311',
                                    cl_ind => 'analysis.2311_flag',
                                    f => 'analysis.2641',
                                    f_ind => 'analysis.2641_flag',
                                    no3 => 'analysis.2331',
                                    no3_ind => 'analysis.2331_flag',
                                    so4 => 'analysis.2401',
                                    so4_ind => 'analysis.2401_flag',
                                    zn => 'analysis.2821',
                                    zn_ind => 'analysis.2821_flag',
                                    al => 'analysis.2501',
                                    al_ind => 'analysis.2501_flag',
                                    b => 'analysis.2551',
                                    b_ind => 'analysis.2551_flag',
                                    cu => 'analysis.2621',
                                    cu_ind => 'analysis.2621_flag',
                                    po4 => 'analysis.3201',
                                    po4_ind => 'analysis.3201_flag',
                                    br => 'analysis.2561',
                                    br_ind => 'analysis.2561_flag',
                                    i => 'analysis.2661',
                                    i_ind => 'analysis.2661_flag',
                                 },
                  water_levels =>  {#WATER_LEVELS.txt
                                    #"RN"|"PIPE"|"RDATE"|"MEAS_POINT"|"MEASUREMENT"|"REMARK"|"LOGGER"
                                    #13600011|A|24/06/1981|R|-17.28|| 
                                    rn => 'gwpipe.station,hydmeas.station', 
                                    pipe => 'hydmeas.pipe,gwpipe.pipe', 
                                    rdate => 'hydmeas.date', 
                                    meas_point => 'hydmeas.variable', 
                                    measurement => 'hydmeas.value', 
                                    remark => 'hydmeas.comment', 
                                    logger => 'hydmeas.source', 
                                  },
                  special_analysis_results =>    {#WQ_SAMPLES_RESULTS_DATA.txt
                                    #"RN"|"PIPE"|"SAMPNUM"|"BOTTLE"|"VARIABLE_NO"|"FLAG"|"VALUE"|"QUALITY"|"COMMNT"
                                    #12600532|A|182289|B|2741.2|<|.004|| 
                                    #I believe this results table needs to have a key that is in accordance with the 
                                    #special_analysis_sample file.
                                    rn => 'results.station,samples.station',
                                    pipe => 'samples.spare3',
                                    sampnum => 'samples.sampnum,results.sampnum',
                                    bottle => 'samples.bottle,results.bottle',
                                    variable_no => 'results.variable',
                                    flag => 'results.flag',
                                    value => 'results.value',
                                    quality => 'results.quality',
                                    commnt => 'results.commnt',
                                 },
                  special_analysis_sample =>    {#WQ_SAMPLES_DATA.txt
                                #"RN"|"PIPE"|"SAMPNUM"|"BOTTLE"|"SDATE"|"PROJECT1"|"SRCSAMP"|"COLLSAMP"|"COLLMETH"|"PRESMETH1"|"REC"|"DEPTH"|"PROJECT2"|"PROJECT3"|"PROJECT4"|"PRESMETH2"|"PRESMETH3"|"PRESMETH4"|"LABREF"|"LABSAMPREF"|"RECDATE"|"INPUTSRC"|"ENTEREDBY"|"CHECKEDBY"|"COMMNT"
                                #13610198|A|210873|D|22/07/2002|GWAN|GB|DG|PW|FR|1|28|||||||GCL||01/08/2002||GW||
                                rn => 'samples.station',
                                pipe => 'samples.spare3',
                                sampnum => 'samples.sampnum',
                                bottle => 'samples.bottle',
                                sdate => 'samples.date',
                                project1 => 'samples.project1',
                                srcsamp => 'samples.srcsamp',
                                collsamp => 'samples.collsamp',
                                collmeth => 'samples.collmeth',
                                presmeth1 => 'samples.presmeth1',
                                rec => 'ignore.rec',
                                depth => 'samples.depth',#set the depth and deal with it as a special case
                                project2 => 'samples.project2',
                                project3 => 'samples.project3',
                                project4 => 'samples.project4',
                                presmeth2 => 'samples.presmeth2',
                                presmeth3 => 'samples.presmeth3',
                                presmeth4 => 'samples.presmeth4',
                                labref => 'samples.labref',
                                labsampref => 'samples.labsampref',
                                recdate => 'samples.recdate',
                                inputsrc => 'samples.inputsrc',
                                enteredby => 'samples.enteredby',
                                checkedby => 'samples.checkedby',
                                commnt => 'samples.commnt'
                             }
              );  
  
  
my %datamappings =(
  hydmeas=>{
    variable=>{
      'r'=>'113.00',
      'n'=>'110.00'
     }
  },
  aquifer=>{
    name=>{
      '?' => '29',
      'aberdare conglomerate' => '15',
      'alluvium' => '01',
      'allvuium' => '01',
      'alluvium       form' => '01',
      'aluv flagstone form' => '29',
      'amberley basin sediments' => '29',
      'back creek alluvium' => '01',
      'n/a' => '29',
      'balonne river alluvium' => '01',
      'basalt' => '11',
      'birkhead formation' => '34',
      'black duck creek alluvium' => '29',
      'blackfellow creek alluvium' => '01',
      'blythesdale group' => '10',
      'boxvale sandstone member' => '04',
      'bremer river alluvium' => '01',
      'buaraba creek alluvium' => '01',
      'bungil' => '12',
      'bungil formation' => '10',
      'burraburri creek alluvium' => '01',
      'cadna-owie formation' => '10',
      'cains creek alluvium' => '01',
      'campbells gully alluvium' => '01',
      'canal creek alluvium' => '01',
      'cattle creek alluvium' => '01',
      'charleys creek alluvium' => '01',
      'n/a' => '29',
      'condamine river alluvium' => '01',
      'cooby creek alluvium' => '01',
      'cooranga creek alluvium' => '01',
      'dalrymple creek alluvium' => '01',
      'dawson river alluvium' => '01',
      'd d basalts    form' => '11',
      'deep creek alluvium' => '01',
      'deep gully alluvium' => '01',
      'doncaster member' => '10',
      'dumaresq river alluvium' => '01',
      'emu creek alluvium' => '01',
      'eurombah creek alluvium' => '01',
      'eurombah formation' => '26',
      'evergreen formation' => '04',
      'farm creek alluvium' => '01',
      'flagstone creek alluvium' => '01',
      'form' => '29',
      'four mile creek alluvium' => '01',
      'franklin vale creek alluvium' => '01',
      'freestone creek alluvium' => '01',
      'gatton creek alluvium' => '01',
      'gatton sandstone' => '06',
      'glengallan creek alluvium' => '01',
      'gomaren creek alluvium' => '01',
      'gowrie creek alluvium' => '01',
      'greymare granodiorite' => '29',
      'griman creek formation' => '10',
      'gubberamunda sandstone' => '05',
      'heifer creek alluvium' => '01',
      'heifer creek sandstone member' => '06',
      'helidon sandstone' => '15',
      'helidon sst    form' => '15',
      'herries adamellite' => '29',
      'hodgson creek alluvium' => '01',
      'hooray sandstone' => '10',
      'hutton sandstone' => '06',
      'injune creek group' => '27',
      'jimbour creek alluvium' => '01',
      'jingi jingi creek alluvium' => '01',
      'juandah creek alluvium' => '01',
      'kings creek alluvium' => '01',
      'koukandowie formation' => '06',
      'kumbarilla beds' => '09',
      'lagoon creek alluvium' => '01',
      'laidley creek alluvium' => '01',
      'limestone creek alluvium' => '01',
      'lockyer creek alluvium' => '01',
      'ma ma creek alluvium' => '01',
      'macintyre brook alluvium' => '01',
      'macintyre river alluvium' => '01',
      'main camp creek alluvium' => '01',
      'main range volcanics' => '11',
      'main range volcanics - undiff.' => '11',
      'mama creek sandstone member' => '01',
      'maranoa river alluvium' => '01',
      'marburg sandstone' => '06',
      'marburg subgroup' => '06',
      'meringandan creek alluvium' => '01',
      'middle creek alluvium' => '01',
      'millarvale creek alluvium' => '01',
      'mingimarny creek alluvium' => '01',
      'mooga sandstone' => '12',
      'moola creek alluvium' => '01',
      'n/a' => '29',
      'moonie river alluvium' => '01',
      'myall creek alluvium' => '01',
      'myall creek north branch alluvium' => '01',
      'neds gully alluvium' => '01',
      'no unit identif' => '29',
      'north myall creek alluvium' => '01',
      'oakey creek alluvium' => '01',
      'oaky creek alluvium' => '01',
      'orallo formation' => '14',
      'precipice sandstone' => '15',
      'plane creek alluvium' => '01',
      'purga creek alluvium' => '01',
      'quaternary - undefined' => '01',
      'n/a' => '29',
      'redbank creek alluvium' => '01',
      'n/a' => '29',
      'reynolds creek alluvium' => '01',
      'robinson creek alluvium' => '01',
      'rodger creek alluvium' => '01',
      'rosenthal creek alluvium' => '01',
      'sandstone' => '29',
      'sandy creek alluvium' => '01',
      'sedimentary - undiff.' => '29',
      'silkstone formation' => '29',
      'southlands formation' => '29',
      'splityard creek alluvium' => '01',
      'spring creek alluvium' => '01',
      'springbok sandstone' => '19',
      'stanthorpe granite' => '29',
      'stockyard creek alluvium' => '01',
      'stuart river alluvium' => '01',
      'n/a' => '29',
      'swan creek alluvium' => '01',
      'tenthill creek alluvium' => '01',
      'teviot brook alluvium' => '01',
      'texas beds' => '25',
      'thanes creek alluvium' => '01',
      'n/a' => '29',
      'n/a' => '29',
      'toowoomba volcanics' => '11',
      'undifferentiated' => '29',
      'n/a' => '29',
      'volcanics - undiff.' => '11',
      'walloon' => '34',
      'walloon coal measures' => '34',
      'wallumbilla formation' => '10',
      'warrill creek alluvium' => '01',
      'warroolaba creek alluvium' => '01',
      'westbrook creek alluvium' => '01',
      'n/a' => '29',
      'western creek alluvium' => '01',
      'wonga creek alluvium' => '01',
      'woogaroo subgroup' => '15',
      'woolshed creek alluvium' => '01',
      'wyandra sandstone member' => '10',
      'aberdare conglomerate' => '33',
      'airlie volcanics' => '33',
      'aldebaran sandstone' => '24',
      'alligator creek alluvium' => '30',
      'alluv' => '30',
      'alluv nago' => '30',
      'alluv quaternary' => '30',
      'alluvial' => '30',
      'alluvium' => '30',
      'alluvium       aluv' => '30',
      'alluvium creek wash' => '30',
      'alluvium/hill wash' => '30',
      'alton downs basalt' => '30',
      'amity creek alluvium' => '30',
      'anakie metamorphic group' => '24',
      'andromache creek alluvium' => '30',
      'andromache river alluvium' => '30',
      'arthur creek alluvium' => '30',
      'back creek group' => '24',
      'back creek group - undiff.' => '24',
      'bagley creek alluvium' => '30',
      'bakers creek alluvium' => '30',
      'bandanna formation' => '33',
      'bas undiff' => '30',
      'basalt' => '30',
      'basalt         beds' => '30',
      'basalts' => '30',
      'bee creek alluvium' => '30',
      'black alley shale' => '24',
      'blackwater group' => '31',
      'blackwater group - undiff.' => '31',
      'blair athol coal measures' => '24',
      'blenheim formation' => '24',
      'boomer formation' => '24',
      'bouldercombe igneous complex' => '30',
      'bowen beds' => '24',
      'bowen vol' => '30',
      'bowen volc' => '30',
      'bulgonunna' => '30',
      'bulgonunna volcanic group' => '30',
      'burngrove formation' => '31',
      'calen coal measures' => '31',
      'camboon volcanics' => '24',
      'campwyn volcanics' => '24',
      'carmila beds' => '24',
      'cattle creek alluvium' => '30',
      'clay' => '33',
      'clematis group' => '33',
      'coal measures' => '31',
      'collinsville granite' => '24',
      'comet river alluvium' => '30',
      'connors river alluvium' => '30',
      'connors volcanic group' => '30',
      'cooper creek alluvium' => '30',
      'craigilee beds' => '24',
      'crana beds' => '24',
      'cret. basalt' => '30',
      'cretaceous basalt' => '30',
      'cretaceous basalts' => '30',
      'cretaceous baslts' => '30',
      'cretaceous flow' => '30',
      'cretaceous sediments' => '33',
      'crinum creek alluvium' => '30',
      'crocker formation' => '24',
      'denison creek alluvium' => '30',
      'develin creek alluvium' => '30',
      'devonian volcanics' => '24',
      'dinner creek conglomerate' => '31',
      'duaringa formation' => '30',
      'exevale formation' => '30',
      'fair hill formation' => '31',
      'fitzroy river alluvium' => '30',
      'fort cooper coal measures' => '31',
      'frietag formation' => '24',
      'funnel creek alluvium' => '30',
      'gebbie formation' => '24',
      'german creek formation' => '24',
      'gilbert creek alluvium' => '30',
      'gowrie creek alluvium' => '30',
      'granite creek alluvium' => '30',
      'granites urrannah' => '30',
      'grasstree creek alluvium' => '30',
      'gyranda subgroup' => '31',
      'harrybrandt creek alluvium' => '30',
      'hecate granite' => '30',
      'ingelara formation' => '24',
      'isaac river alluvium' => '30',
      'kelsey creek alluvium' => '30',
      'langdale hill rhyolite' => '24',
      'lethe brook alluvium' => '30',
      'leura volcanics' => '30',
      'lizzie creek volcanic group' => '24',
      'lotus creek alluvium' => '30',
      'louisa formation' => '30',
      'lowen bowen volform' => '30',
      'lower bowen volc' => '30',
      'lower bowen volcan' => '30',
      'lower bowen volcanic' => '30',
      'lower bowen volcanics' => '30',
      'lower bowen volcs' => '30',
      'lower carb' => '24',
      'lower devonian' => '24',
      'lowerbowen' => '30',
      'lowr bowen' => '30',
      'lucy creek alluvium' => '30',
      'mackay microdiorite' => '30',
      'mackenzie river alluvium' => '30',
      'mamelon creek alluvium' => '30',
      'marlborough alluvium' => '30',
      'marlborough serpentinite' => '33',
      'mcgregor creek alluvium' => '30',
      'miclere ck  alluv' => '30',
      'miclere creek alluvium' => '30',
      'middbowenb' => '30',
      'middle creek alluvium' => '30',
      'moah creek alluvium' => '30',
      'moah creek beds' => '24',
      'moolayember creek alluvium' => '30',
      'moranbah coal measures' => '28',
      'mount alma formation' => '24',
      'mount buffalo rhyolite' => '24',
      'mount hall formation' => '24',
      'mount salmon volcanics' => '30',
      'mount view volcanics' => '30',
      'mountain view volcanics' => '30',
      'mt vince granites' => '30',
      'native cat andesite' => '33',
      'navada granites' => '33',
      'nebo creek' => '30',
      'nebo creek alluvium' => '30',
      'neerkol formation' => '24',
      'new chum formation' => '30',
      'no data' => '30',
      'nogoa river alluvium' => '30',
      'oaky creek alluvium' => '30',
      'oconnell river alluvium' => '30',
      'paddock creek formation' => '24',
      'palm tree creek alluvium' => '30',
      'peawaddy formation' => '24',
      'permian - undefined' => '31',
      'permian coal measure' => '31',
      'permian granite' => '31',
      'permian granites' => '31',
      'perry creek alluvium' => '30',
      'pioneer river alluvium' => '30',
      'princhester serpentinite' => '24',
      'proserpine river alluvium' => '30',
      'quarternary' => '30',
      'quaternary' => '30',
      'quaternary     aluv' => '30',
      'quaternary - undefined' => '30',
      'rangal coal measures' => '32',
      'rannes beds' => '24',
      'reids dome beds' => '31',
      'retreat granite' => '24',
      'rewan group' => '33',
      'rhyolite range beds' => '33',
      'ridgelands granodiorite' => '33',
      'rock creek alluvium' => '30',
      'rockhampton group' => '24',
      'rookwood volcanics' => '31',
      'ross creek alluvium' => '30',
      'sandy ck' => '30',
      'sandy claystone' => '30',
      'sandy creek alluvium' => '30',
      'sawn bridge creek alluvium' => '30',
      'sedimentary - undiff.' => '33',
      'sediments' => '33',
      'serpentinite' => '24',
      'silver hills volcanics' => '24',
      'slatey creek   alluv' => '30',
      'slatey creek alluvium' => '30',
      'st lawrence alluvium' => '30',
      'stockyard creek alluvium' => '30',
      'styx coal measures' => '31',
      'styx river alluvium' => '30',
      'suttor formation' => '30',
      'suttor river alluvium' => '30',
      'teriary basalt' => '30',
      'teriary sediments' => '30',
      'tert' => '30',
      'tert basalt' => '30',
      'tert basalts' => '30',
      'tert seds' => '30',
      'tert siltstone grou' => '30',
      'tert.basalt' => '30',
      'tert.baslat' => '30',
      'tert.sediments' => '30',
      'tert.sediments  ???' => '30',
      'tertbasalt' => '30',
      'tertiarty sediments' => '30',
      'tertiary' => '30',
      'tertiary - undefined' => '30',
      'tertiary alluvium' => '30',
      'tertiary badsalt' => '30',
      'tertiary basalt' => '30',
      'tertiary basalts' => '30',
      'tertiary beds' => '30',
      'tertiary deposits' => '30',
      'tertiary formation' => '30',
      'tertiary sand' => '30',
      'tertiary sands/bslt' => '30',
      'tertiary sediments' => '30',
      'tertiary sediments??' => '30',
      'theresa creek alluvium' => '30',
      'two mile creek alluvium' => '30',
      'ukalunda beds' => '30',
      'undiff   basalt' => '30',
      'undiff  basalt' => '30',
      'undiff  basaltt' => '30',
      'undiff  sandstone' => '30',
      'undiff  tert' => '30',
      'undiff  tert     e' => '30',
      'undiff bas' => '30',
      'undiff basalt' => '30',
      'undiff coal measures' => '31',
      'upper bowen coal' => '31',
      'upper bowen coal measures' => '31',
      'upper carb' => '30',
      'urannah' => '30',
      'urannah granites' => '30',
      'urannah igneous complex' => '30',
      'urranah granite' => '30',
      'west funnel creek alluvium' => '30',
      'winton formation' => '24',
      'wolfang creek alluvium' => '30',
      'yarrol basin sequence' => '30',
      'youlambie conglomerate' => '33'
    }
  },
  
);  
my $tableCount = 0;
foreach my $table ( keys %data_type_mapping ){
  my $data;
  $tableCount++;
  open my $fh, ">", DB_DIR."$table.json";
  
  my $uctable = uc($table);
  my $primary_key = 'PRIMARY KEY (';
  my $create;
  my $vals;
  
  my @fields = @{$data_type_mapping{$table}} ;
  foreach my $fld ( 0..$#fields ) {
    print "fd [$fld]\n";
    foreach my $field ( keys %{$fields[$fld] } ){
      print "field [$field]\n";
      my @table_fields = split (/\,/,$mappings{lc($table)}{lc($field)} );
      foreach my $tf ( 0..$#table_fields){
        my ($hytable,$hyfield) = split (/\./,$table_fields[$tf] );
        if ( $hytable eq 'analysis'){
          
          my ($field, $value, $placeholder);
          if ( $hyfield =~ m{_flag$} )
          { 
           ($value,$field) = split(/_/,lc($hyfield));
          $value = sprintf("%.2f",$value);
          }
          else{
           $value = sprintf("%.2f",$hyfield);
           $field = 'value';
          }
          
          
          $data->{'elements'}[$fld]{'hydstra_mappings'}[$tf]{'table'} = 'results';
          $data->{'elements'}[$fld]{'hydstra_mappings'}[$tf]{'field'} = $field;

          $data->{'elements'}[$fld]{'hydstra_mappings'}[$tf+1]{'table'} = 'results';
          $data->{'elements'}[$fld]{'hydstra_mappings'}[$tf+1]{'field'} = 'variable';
          $data->{'elements'}[$fld]{'hydstra_mappings'}[$tf+1]{'value'} = $value;
        
        }
        elsif( $hytable eq 'ignore'){
          next;
        }
        else{
        
          $data->{'elements'}[$fld]{'hydstra_mappings'}[$tf]{'table'} = lc($hytable);
          $data->{'elements'}[$fld]{'hydstra_mappings'}[$tf]{'field'} = lc($hyfield);
          foreach my $value ( keys %{$datamappings{lc($hytable)}{lc($hyfield)}} ){
            $data->{'elements'}[$fld]{'hydstra_mappings'}[$tf]{'value_mappings'}{$value} = $datamappings{$hytable}{$hyfield}{$value};
          }
        }
      }
      
      my $key = ( defined $primary_keys{$table}{$field} )? 1 : 0;
      #$data->{'elements'}[$fld]{'default'} = '';
      $data->{'elements'}[$fld]{'foreign_field'} = $field;
      $data->{'elements'}[$fld]{'foreign_table'} = $table;
      $data->{'elements'}[$fld]{'foreign_key_field'} = $key;
      $data->{'elements'}[$fld]{'foreign_sqlite_type'} = $fields[$fld]{$field};
      #$data->{'elements'}[$fld]{'hydstra_table'} = $hytable;
      my $ucfirst_field = ucfirst($field);
      $primary_key .= ( defined $primary_keys{$table}{$field} )? "$ucfirst_field, " :'';
      my $sql_type = uc($fields[$fld]{$field});
      $create .= " $ucfirst_field $sql_type, ";
      $vals .= '?, '; 
    }
    
  }
  $primary_key =~ s{\, $}{\)};
  $create =~ s{\, $}{};
  $vals =~ s{\, $}{};
  
  $data->{'foreign_table_sqlcreate'} = "CREATE TABLE IF NOT EXISTS FORIEGN_$uctable ($create, $primary_key)";
  $data->{'foreign_table_sqlprepare'} = "INSERT INTO FORIEGN_$uctable VALUES ($vals)";
  $data->{'foreign_table_sqlmapping'} = "INSERT INTO SITE (SITE, LATITUDE, LONGITUDE) SELECT (RN, LATITUDE, LONGITUDE ) FROM FORIEGN_$uctable";
  $data->{'foreign_table_name'} = $table;
  $data->{'foreign_table_multiple_lines'} = 1;
  
  print $fh encode_json($data);
}
my $out = DB_DIR;
print "output to [$out]";

1;  
  