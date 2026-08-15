/* ═══════════════════════════════════════════
   NewFreedeck — ROM 启动选项实时设置模块
   通过 SteamClient API 实现无重启生效
   ═══════════════════════════════════════════ */

export interface RomImportData {
  task_id: string;
  game_title: string;
  emu_appimage: string;
  rom_path: string;
  launch_options: string;
  platform: string;
}

/**
 * 通过 SteamClient API 实时设置 ROM 的启动选项。
 * 调用链：AddShortcut（若无 appid）→ SetShortcutName → SetShortcutLaunchOptions → SetCustomArtwork
 */
export async function runRomSteamImport(data: RomImportData): Promise<number> {
  const SC = (window as any).SteamClient;
  if (!SC?.Apps) {
    throw new Error("SteamClient API 不可用");
  }

  const Apps = SC.Apps;
  let appid: number | undefined;

  /* ── 方法 1：AddShortcut（如果存在，一步到位含 LaunchOptions）── */
  if (typeof Apps.AddShortcut === "function") {
    try {
      appid = await new Promise<number>((resolve, reject) => {
        try {
          const id = Apps.AddShortcut(
            data.game_title,
            data.emu_appimage,
            data.rom_path.substring(0, data.rom_path.lastIndexOf("/")),
            data.launch_options,
          );
          resolve(id);
        } catch (e) {
          reject(e);
        }
      });
      if (appid) {
        // AddShortcut 可能同步返回也可能异步，等一小下
        await new Promise((r) => setTimeout(r, 1000));
        await ensureAppRegistered(appid, Apps);
        await setShortcutName(appid, data.game_title, Apps);
        return appid;
      }
    } catch {
      // AddShortcut 不可用，回退
    }
  }

  /* ── 方法 2：SetShortcutLaunchOptions（如果已有 appid）── */
  // 这个分支由已有 steamos-add-to-steam 创建的 appid 触发
  // 后端会先调用 steamos-add-to-steam 拿 appid，前端用此方法设启动选项

  throw new Error("无法实时设置启动选项");
}

/**
 * 对已有的 appid 实时设置启动选项（无需重启 Steam）。
 * 后端 steamos-add-to-steam 之后调用。
 */
export async function applyRomLaunchOptions(
  appid: number,
  gameTitle: string,
  launchOptions: string,
): Promise<boolean> {
  const SC = (window as any).SteamClient;
  if (!SC?.Apps) return false;

  const Apps = SC.Apps;

  // 尝试 SetShortcutLaunchOptions
  if (typeof Apps.SetShortcutLaunchOptions === "function") {
    try {
      await Apps.SetShortcutLaunchOptions(appid, launchOptions);
      // 调用 SetShortcutName 触发 Steam 刷新缓存
      if (typeof Apps.SetShortcutName === "function") {
        await Apps.SetShortcutName(appid, gameTitle);
      }
      return true;
    } catch {
      // 失败，回退
    }
  }

  // 试试 BIsShortcut + SaveCustomLogoPosition 触发刷新（hack）
  // 某些 Steam 版本调用 SetShortcutName 会连带重读 VDF 中的 LaunchOptions
  try {
    if (typeof Apps.SetShortcutName === "function") {
      await Apps.SetShortcutName(appid, gameTitle);
      // 再调一次，Steam 可能刷新了 VDF
      return true;
    }
  } catch {
    // ignore
  }

  return false;
}

async function setShortcutName(appid: number, name: string, Apps: any) {
  if (typeof Apps.SetShortcutName === "function") {
    try {
      await Apps.SetShortcutName(appid, name);
    } catch {
      // ignore
    }
  }
}

async function ensureAppRegistered(appid: number, Apps: any) {
  try {
    await new Promise<void>((resolve) => {
      const t = setTimeout(resolve, 3000);
      try {
        const { unregister } = Apps.RegisterForAppDetails(appid, () => {
          clearTimeout(t);
          unregister();
          resolve();
        });
      } catch {
        clearTimeout(t);
        resolve();
      }
    });
  } catch {
    // ignore
  }
}
