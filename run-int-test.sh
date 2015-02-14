export MONO_THREADS_PER_CPU=200
clear
xbuild src/FsRaft.Program/FsRaft.Program.fsproj
mono-sgen --server src/FsRaft.Program/bin/Debug/FsRaft.Program.exe /verbosity:quiet
