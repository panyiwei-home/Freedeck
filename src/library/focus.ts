export function findNavigableElement(root: HTMLElement | null | undefined): HTMLElement | null {
  const node = root || null;
  if (!node) return null;
  if (typeof node.focus === "function" && node.tabIndex >= 0) return node;
  return (
    (node.querySelector(
      'button, [role="button"], [tabindex]:not([tabindex="-1"]), a[href], input, textarea, select',
    ) as HTMLElement | null) || node
  );
}

export function focusNavigableElement(root: HTMLElement | null | undefined): boolean {
  const target = findNavigableElement(root);
  if (!target) return false;
  window.requestAnimationFrame(() => {
    target.scrollIntoView({ block: "nearest", inline: "nearest" });
    target.focus?.();
  });
  return true;
}

export function focusNavigableElementWithRetry(
  resolver: () => HTMLElement | null | undefined,
  attempt = 0,
  maxAttempts = 6,
): boolean {
  const target = resolver();
  if (focusNavigableElement(target)) return true;
  if (attempt >= maxAttempts) return false;
  window.requestAnimationFrame(() => {
    focusNavigableElementWithRetry(resolver, attempt + 1, maxAttempts);
  });
  return false;
}
