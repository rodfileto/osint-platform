import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  /* config options here */
  webpack(config: Parameters<NonNullable<NextConfig["webpack"]>>[0]) {
    config.module.rules.push({
      test: /\.svg$/,
      use: ["@svgr/webpack"],
    });
    return config;
  },
    
    turbopack: {
      rules: {
        '*.svg': {
          loaders: ['@svgr/webpack'],
          as: '*.js',
        },
      },
    },
  
};

export default nextConfig;
