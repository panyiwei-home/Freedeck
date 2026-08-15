declare module "react-router" {
  import type { ComponentType, ReactNode } from "react";

  export interface RouteProps {
    path?: string;
    exact?: boolean;
    strict?: boolean;
    sensitive?: boolean;
    children?: ReactNode;
    component?: ComponentType<any>;
    render?: (...args: any[]) => ReactNode;
    [key: string]: unknown;
  }
}

declare module '*.webp' { const src: string; export default src; }
declare module '*.svg'  { const src: string; export default src; }
declare module '*.png'  { const src: string; export default src; }
