<script setup>
defineProps({
  title: String,
  value: String,
  subtitle: String
})
</script>

<template>
  <div class="bg-white p-6 rounded-lg shadow-sm w-full">
    <h3 class="text-sm font-medium text-gray-500 mb-1">{{ title }}</h3>
    <div class="text-3xl font-bold text-gray-900 mb-1">{{ value }}</div>
    <div v-if="subtitle" class="text-sm text-gray-400">{{ subtitle }}</div>
  </div>
</template>
