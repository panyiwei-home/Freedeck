import { useCallback, useEffect, useState } from "react";
import { checkLogin, getPanelState } from "./api";
import type { LoginState } from "./api";

const EMPTY_LOGIN_STATE: LoginState = {
  logged_in: false,
  account_name: "",
  message: "",
};

const VALIDATE_COOLDOWN_MS = 15_000;

type LoginListener = (next: LoginState | null) => void;

let cachedLoginState: LoginState | null = null;
let lastValidatedAt = 0;
let bootstrapPromise: Promise<LoginState | null> | null = null;
let validatePromise: Promise<LoginState | null> | null = null;
const listeners = new Set<LoginListener>();

function normalizeLoginState(next?: LoginState | null): LoginState {
  return {
    logged_in: Boolean(next?.logged_in),
    account_name: String(next?.account_name || ""),
    message: String(next?.message || ""),
  };
}

function emitLoginState(next: LoginState | null): void {
  for (const listener of Array.from(listeners)) {
    try {
      listener(next);
    } catch {
      // ignore listener failure
    }
  }
}

export function setLoginState(next?: LoginState | null): LoginState {
  cachedLoginState = normalizeLoginState(next || EMPTY_LOGIN_STATE);
  emitLoginState(cachedLoginState);
  return cachedLoginState;
}

async function bootstrapLoginState(): Promise<LoginState | null> {
  if (cachedLoginState) return cachedLoginState;
  if (!bootstrapPromise) {
    bootstrapPromise = (async () => {
      try {
        const result = await getPanelState();
        if (result?.status === "success" && result.data?.login) {
          return setLoginState(result.data.login);
        }
        return null;
      } catch {
        return null;
      } finally {
        bootstrapPromise = null;
      }
    })();
  }
  return bootstrapPromise;
}

async function validateLoginState(force = false): Promise<LoginState | null> {
  const now = Date.now();
  if (!force && cachedLoginState && now - lastValidatedAt < VALIDATE_COOLDOWN_MS) {
    return cachedLoginState;
  }
  if (!validatePromise) {
    validatePromise = (async () => {
      try {
        const payload = force ? { force: true } : undefined;
        const result = await checkLogin(payload);
        if (result?.status === "success" && result.data) {
          lastValidatedAt = Date.now();
          return setLoginState(result.data);
        }
        return cachedLoginState;
      } catch {
        return cachedLoginState;
      } finally {
        validatePromise = null;
      }
    })();
  }
  return validatePromise;
}

type UseLoginStateOptions = {
  validateOnMount?: boolean;
  refreshOnVisible?: boolean;
};

export function useLoginState(options: UseLoginStateOptions = {}) {
  const { validateOnMount = true, refreshOnVisible = false } = options;
  const [login, setLogin] = useState<LoginState | null>(() => cachedLoginState);

  useEffect(() => {
    listeners.add(setLogin);
    if (cachedLoginState) {
      setLogin(cachedLoginState);
    } else {
      void bootstrapLoginState();
    }
    if (validateOnMount) {
      void validateLoginState(cachedLoginState === null);
    }
    return () => {
      listeners.delete(setLogin);
    };
  }, [validateOnMount]);

  useEffect(() => {
    if (!refreshOnVisible) return;
    const handleVisible = () => {
      if (!document.hidden) {
        void validateLoginState(false);
      }
    };
    const handleFocus = () => {
      void validateLoginState(false);
    };
    document.addEventListener("visibilitychange", handleVisible);
    window.addEventListener("focus", handleFocus);
    return () => {
      document.removeEventListener("visibilitychange", handleVisible);
      window.removeEventListener("focus", handleFocus);
    };
  }, [refreshOnVisible]);

  const refresh = useCallback((force = true) => validateLoginState(force), []);

  return {
    login,
    loading: login === null,
    refresh,
    setLoginState,
  };
}
