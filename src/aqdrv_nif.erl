-module(aqdrv_nif).
-export([init/1, open/1, stage_write/2, write/5]).

open(_) ->
	exit(nif_library_not_loaded).
stage_write(_,_) ->
	exit(nif_library_not_loaded).
write(_,_,_,_,_) ->
	exit(nif_library_not_loaded).

init(Info) ->
	NifName = "aqdrv_nif",
	NifFileName = case code:priv_dir(aqdrv) of
		{error, bad_name} -> filename:join("priv", NifName);
		Dir -> filename:join(Dir, NifName)
	end,
	case file:read_file_info(NifFileName) of
		{error,_} ->
			FN = filename:join("../priv/",NifName);
		_ ->
			FN = NifFileName
	end,
	case erlang:load_nif(FN, Info#{logname => "drv_"++hd(string:tokens(atom_to_list(node()),"@"))++".txt"}) of
		ok ->
			ok;
		{error,{upgrade,_}} ->
			ok;
		{error,{reload,_}} ->
			ok
	end.
