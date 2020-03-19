\d .importer

fetch_data:{[table_names;batch_size;data_date]
    / (1) Open remote connection
    h::hopen `$":redacted:redacted:redacted";
    
    / (2) Set arguments
    tabs:enlist table_names;
    bsize::batch_size;
    dday::data_date;  
  
    / (3) For each remote table;
    /     (3.1) Get the table count
    /     (3.2) Split the table to an optimal value
    /     (3.3) Fetch the meta to create a proper CSV
    /     (3.4) Open file handle
    /     (3.5) Iterate over the table by using the splits and for each batch
    /           (3.5.1) Asynchronously save data to the CSV file
    {[tab]
      tcount:h({[t;d] count select from t where date=d};tab;dday);
      splits:$[tcount>bsize; [batch:tcount div bsize;((0;bsize-1)+/:bsize*til batch),enlist (batch*bsize;tcount-1)];enlist(0;tcount-1)];
      tmeta: key h({[t] meta t};tab);         
      (hsym `$ raze string tab,".csv") 0: enlist "," sv raze string flip tmeta cols tmeta;
      fhandle:hopen (hsym `$ raze string tab,".csv");      
      {[t;dday;tcount;fhandle;split] 
        neg[fhandle] peach 1_"," 0:h({[t;y;dday] ?[t;((=;`date;dday);(within;`i;y));0b;()]};t;split;dday); 
        show raze string split[0],"-",split[1]," of ", tcount; }[tab;dday;tcount;fhandle] each splits;
      hclose fhandle; 
    }each tabs;
    hclose h;
  }

fetch_today:fetch_data[;;.z.d];
fetch_quotes_today:fetch_data[`quotes;;.z.d];
fetch_quotes:fetch_data[`quotes;;];
