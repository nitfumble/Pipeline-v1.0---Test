; STACKS installer. Builds STACKS-Setup.exe from the cleaned, source-only
; repo tree. Requires Inno Setup 6+ (https://jrsoftware.org/isinfo.php).
;
; Build with:  "C:\Program Files\Inno Setup 7\ISCC.exe" installer\stacks.iss
; Output lands in installer\output\STACKS-Setup.exe.
;
; WSL/Python/dependency provisioning is deliberately NOT written in Pascal
; Script here -- it lives in Setup-Prerequisites.ps1 (+ bootstrap_env.sh),
; which can be run and debugged standalone from a terminal. This script's
; job is just: copy files, run that script once post-install with a
; progress page, create shortcuts.

#define MyAppName "STACKS"
#define MyAppVersion "1.0"
#define MyAppPublisher "Nitfumble"

[Setup]
AppId={{B3B4F3B1-7B1B-4B1A-9C1A-STACKS000001}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes
PrivilegesRequired=admin
ArchitecturesInstallIn64BitMode=x64compatible
OutputDir=output
OutputBaseFilename=STACKS-Setup
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
; Having this set means a repair/repeat run (including the post-reboot
; RunOnce resume) lands on the same install directory automatically.
UsePreviousAppDir=yes

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop shortcut"; GroupDescription: "Additional shortcuts:"

[Files]
; The cleaned, source-only repo tree. Mirrors ../.gitignore exactly, PLUS
; config.json/secrets.json: those are only gitignored (removed from git
; tracking, not deleted from the dev's disk), so [Files] -- which copies
; whatever's physically present regardless of git state -- would otherwise
; bundle the dev's real, live secrets and machine-specific config straight
; into the installer. ML models and slskd's exe/zip are fetched at install
; time (Fetch-Assets.ps1) instead of bundled; slskd's LICENSE and example
; config are small tracked source files and DO ship.
Source: "..\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs; Excludes: ".git\*,installer\output\*,__pycache__\*,*.pyc,.cache\*,audio\*,bin\*,source\*,db\*,backups\*,logs\*,playlists\*,tmp\*,models\*,slskd\*.exe,slskd\*.zip,slskd\wwwroot\*,slskd\etc\*,slskd\*.json,.venv\*,.claude\*,config.json,secrets.json,*.wav,*.mp3,*.flac,*.aiff,*.db,*.sqlite"

[Icons]
Name: "{group}\{#MyAppName}"; Filename: "{app}\start_stacks.bat"; WorkingDir: "{app}"
Name: "{group}\Uninstall {#MyAppName}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\start_stacks.bat"; WorkingDir: "{app}"; Tasks: desktopicon

[Run]
Filename: "{app}\start_stacks.bat"; Description: "Launch {#MyAppName} now"; Flags: postinstall skipifsilent nowait

[Code]
var
  ProgressPage: TOutputProgressWizardPage;
  PrereqsNeedReboot: Boolean;

procedure InitializeWizard;
begin
  ProgressPage := CreateOutputProgressPage(
    'Setting Up STACKS',
    'Installing WSL, Python 3.11, and dependencies inside it. The first run can take several minutes.');
end;

{ Reads "<percent>|<phase>" from StatusFile and reflects it on ProgressPage.
  Returns True once DoneFile appears. }
function PollPrerequisites(const StatusFile, DoneFile: String): Boolean;
var
  Line: AnsiString;
  SepPos: Integer;
  Percent: Integer;
begin
  Result := FileExists(DoneFile);
  if LoadStringFromFile(StatusFile, Line) then
  begin
    SepPos := Pos('|', String(Line));
    if SepPos > 0 then
    begin
      Percent := StrToIntDef(Copy(String(Line), 1, SepPos - 1), 0);
      ProgressPage.SetText(Copy(String(Line), SepPos + 1, MaxInt), '');
      ProgressPage.SetProgress(Percent, 100);
    end;
  end;
end;

{ Runs Setup-Prerequisites.ps1, polling its status file until it signals
  completion. Returns 'OK', 'REBOOT', or 'FAIL|<message>'. }
function RunPrerequisites(): String;
var
  StatusFile, DoneFile, PSExe, Params: String;
  DoneContents: AnsiString;
  ResultCode: Integer;
begin
  StatusFile := ExpandConstant('{commonappdata}\STACKS\status.txt');
  DoneFile := ExpandConstant('{commonappdata}\STACKS\done.txt');
  DeleteFile(DoneFile);

  PSExe := ExpandConstant('{sys}\WindowsPowerShell\v1.0\powershell.exe');
  Params := '-NoProfile -ExecutionPolicy Bypass -File "' +
    ExpandConstant('{app}\installer\Setup-Prerequisites.ps1') + '" ' +
    '-AppDir "' + ExpandConstant('{app}') + '" ' +
    '-SrcExe "' + ExpandConstant('{srcexe}') + '"';

  ProgressPage.Show;
  try
    ProgressPage.SetText('Starting...', '');
    ProgressPage.SetProgress(0, 100);
    if not Exec(PSExe, Params, '', SW_HIDE, ewNoWait, ResultCode) then
    begin
      Result := 'FAIL|Could not launch PowerShell to run prerequisites.';
      Exit;
    end;

    { Setup-Prerequisites.ps1 writes DoneFile exactly once, at the very end
      (success, reboot-needed, or failure) -- wait for it rather than for
      the process handle, since ewNoWait doesn't give us one to poll. }
    while not PollPrerequisites(StatusFile, DoneFile) do
      Sleep(400); { Inno's Sleep pumps messages, so the wizard stays responsive }

    if not LoadStringFromFile(DoneFile, DoneContents) then
      DoneContents := 'FAIL|Prerequisites script did not report a result.';
    Result := String(DoneContents);
  finally
    ProgressPage.Hide;
  end;
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  Outcome: String;
begin
  if CurStep = ssPostInstall then
  begin
    Outcome := RunPrerequisites();
    if Outcome = 'OK' then
    begin
      { nothing further to do -- the [Run] entry launches STACKS on Finish }
    end
    else if Outcome = 'REBOOT' then
    begin
      PrereqsNeedReboot := True;
    end
    else
    begin
      MsgBox('Setting up STACKS hit a problem:' + #13#10 + #13#10 + Outcome + #13#10 + #13#10 +
        'You can re-run this installer to retry once the issue above is resolved.',
        mbError, MB_OK);
    end;
  end;
end;

function NeedRestart(): Boolean;
begin
  Result := PrereqsNeedReboot;
end;
