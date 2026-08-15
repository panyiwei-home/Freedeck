/* ═══════════════════════════════════════════
   NewFreedeck — 类型定义
   ═══════════════════════════════════════════ */

export type LoginState = {
  logged_in?: boolean;
  account_name?: string;
  message?: string;
};

export type QrState = {
  session_id?: string;
  stage?: string;
  message?: string;
  reason?: string;
  user_account?: string;
  qr_data_url?: string;
};

export type ApiResponse<T> = {
  status?: string;
  data?: T;
  message?: string;
};

export type CatalogItem = {
  game_id: string;
  title: string;
  title_zh: string;
  title_en: string;
  platform: string;
  cover_url: string;
  down_url: string;
  pwd: string;
  size_text: string;
  size_bytes: number;
  server?: string;
  version?: string;
  file_name?: string;
  validity?: string;
  genre?: string;
  install_size_text?: string;
  install_size_bytes?: number;
  // 分组合并
  grouped?: boolean;
  variant_count?: number;
  multi_disc?: boolean;
  disc_count?: number;
  variants?: CatalogItem[];
};
