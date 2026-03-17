import { NextRequest, NextResponse } from "next/server";

const CANONICAL_HOST = "microflowops.com";
const WWW_HOST = "www.microflowops.com";

export function proxy(request: NextRequest) {
  const host = request.headers.get("host") || "";
  const hostname = host.split(":")[0].toLowerCase();

  if (hostname === WWW_HOST) {
    const url = request.nextUrl.clone();
    url.hostname = CANONICAL_HOST;
    return NextResponse.redirect(url, 301);
  }

  return NextResponse.next();
}

export const config = {
  matcher: ["/:path*"]
};

