/**
 * DSH bundle marker for the Neo-MoFox integration package.
 *
 * The runtime bridge is a Neo-MoFox Python plugin. This module deliberately
 * performs no DSH-side I/O: its sole purpose is to make the package visible in
 * a DSH profile after `dsh plugin add`, while the Python files are deployed to
 * Neo-MoFox according to the README.
 */
export const name = "dsh-for-mofox-ada";

/**
 * Register the package as a harmless DSH profile bundle.
 *
 * @param {unknown} _ctx Cordis context supplied by DSH.
 */
export function apply(_ctx) {}