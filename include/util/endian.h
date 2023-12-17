//
// Created by 115282 on 2023/10/17.
//

#ifndef KV_STORE_INCLUDE_UTIL_ENDIAN_H_
#define KV_STORE_INCLUDE_UTIL_ENDIAN_H_

namespace Utils
{
    namespace Endian
    {
#if (__BYTE_ORDER == __BIG_ENDIAN)
        /* Toggle the 16 bit unsigned integer pointed by *p from little endian to
         * big endian */
        inline void memConv16 (void *p)
        {
            unsigned char *x = reinterpret_cast<unsigned char *>(p), t;

            t = x[0];
            x[0] = x[1];
            x[1] = t;
        }

        /* Toggle the 32 bit unsigned integer pointed by *p from little endian to
         * big endian */
        inline void memConv32 (void *p)
        {
            unsigned char *x = reinterpret_cast<unsigned char *>(p), t;

            t = x[0];
            x[0] = x[3];
            x[3] = t;
            t = x[1];
            x[1] = x[2];
            x[2] = t;
        }

        /* Toggle the 64 bit unsigned integer pointed by *p from little endian to
         * big endian */
        inline void memConv64 (void *p)
        {
            unsigned char *x = reinterpret_cast<unsigned char *>(p), t;

            t = x[0];
            x[0] = x[7];
            x[7] = t;
            t = x[1];
            x[1] = x[6];
            x[6] = t;
            t = x[2];
            x[2] = x[5];
            x[5] = t;
            t = x[3];
            x[3] = x[4];
            x[4] = t;
        }

        inline int16_t intConv16 (int16_t v)
        {
            memConv16(&v);
            return v;
        }

        inline int32_t intConv32 (int32_t v)
        {
            memConv32(&v);
            return v;
        }

        inline int64_t intConv64 (int64_t v)
        {
            memConv64(&v);
            return v;
        }

#elif (__BYTE_ORDER == __LITTLE_ENDIAN)
        inline void memConv16 (void *p) {}
        inline void memConv32 (void *p) {}
        inline void memConv64 (void *p) {}
        inline int16_t intConv16 (int16_t v) { return v; }
        inline int32_t intConv32 (int32_t v) { return v; }
        inline int64_t intConv64 (int64_t v) { return v; }
#else
        static_assert(false, "unknown endian");
#endif
    }
}
#endif //KV_STORE_INCLUDE_UTIL_ENDIAN_H_
